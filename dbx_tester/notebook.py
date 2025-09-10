from __future__ import annotations

from dbx_tester.global_config import GlobalConfigManager
from dbx_tester.config_manager import NotebookConfigManager
from dbx_tester.utils.databricks_api import (
    get_notebook_path, 
    create_notebook, 
    submit_run, 
    is_notebook, 
    run_notebook
)
from dbx_tester.utils.databricks_dbutils import get_param

from pathlib import Path
from collections.abc import Callable
from typing import Type, Any, List, Dict, Literal, Optional, Union
from datetime import datetime
from dataclasses import dataclass, field
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class NotebookNode:
    """Represents a node in the notebook execution graph."""
    task_name: str
    notebook: create_notebook
    type: Literal["notebook", "task"] = "notebook"
    cluster: Optional[str] = None


@dataclass
class NotebookGraph:
    """Represents the execution graph of notebooks and their dependencies."""
    nodes: Dict[str, NotebookNode] = field(default_factory=dict)
    edges: Dict[str, List[str]] = field(default_factory=dict)


class NotebookValidationError(ValueError):
    """Custom exception for notebook validation errors."""
    pass


class Notebook:
    """Represents a notebook with its dependencies and configuration."""
    
    def __init__(
        self, 
        notebook_path: str, 
        task_name: Optional[str] = None,
        config: Optional[NotebookConfigManager] = None, 
        cluster: Optional[str] = None, 
        depends_on: Optional[Union[Notebook, List[Notebook]]] = None
    ):
        self.notebook_path = notebook_path
        self.task_name = task_name
        self.config = config
        self.cluster = cluster
        self.depends_on = self._normalize_dependencies(depends_on)
        self.notebook_graph = NotebookGraph()
        self.global_config = self._initialize_global_config()
        
        self._validate_and_resolve_paths()
        self._validate_inputs()
        self._initialize_task_name()
        self._create_main_notebook()

    def _normalize_dependencies(
        self, 
        depends_on: Optional[Union[Notebook, List[Notebook]]]
    ) -> List[Notebook]:
        """Normalize dependencies to always be a list."""
        if depends_on is None:
            return []
        if isinstance(depends_on, Notebook):
            return [depends_on]
        if isinstance(depends_on, list):
            self._validate_dependency_list(depends_on)
            return depends_on
        raise NotebookValidationError(
            "depends_on must be a Notebook instance or list of Notebook instances"
        )

    def _initialize_global_config(self) -> GlobalConfigManager:
        """Initialize and load global configuration."""
        config = GlobalConfigManager()
        config._load_config()
        return config

    def _validate_dependency_list(self, depends_on: List[Any]) -> None:
        """Validate that all items in dependency list are Notebook instances."""
        for item in depends_on:
            if not isinstance(item, Notebook):
                raise NotebookValidationError(
                    "All items in depends_on list must be Notebook instances"
                )

    def _validate_and_resolve_paths(self) -> None:
        """Validate and resolve notebook paths."""
        if self.notebook_path is None:
            return
            
        path = Path(self.notebook_path)
        repo_path = Path(self.global_config.REPO_PATH) if self.global_config.REPO_PATH else None
        
        if not is_notebook(path=path.as_posix()) and repo_path:
            resolved_path = repo_path / path
            if is_notebook(path=resolved_path.as_posix()):
                self.notebook_path = resolved_path.as_posix()
            else:
                raise NotebookValidationError(
                    f"Notebook path does not exist: {self.notebook_path}"
                )
        else:
            raise NotebookValidationError(
                f"Notebook path does not exist: {self.notebook_path}"
            )

    def _validate_inputs(self) -> None:
        """Validate input parameters."""
        if self.task_name is not None and (
            not isinstance(self.task_name, str) or not self.task_name.strip()
        ):
            raise NotebookValidationError("task_name must be a non-empty string")
            
        if self.config is not None and not isinstance(self.config, NotebookConfigManager):
            raise NotebookValidationError(
                "config must be a NotebookConfigManager instance"
            )

    def _initialize_task_name(self) -> None:
        """Initialize task name if not provided."""
        if self.task_name is None:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            notebook_name = Path(self.notebook_path).stem
            self.task_name = f"{notebook_name}_task_{timestamp}"

    def _create_main_notebook(self) -> None:
        """Create the main notebook and initialize the graph."""
        self.main_notebook = create_notebook(self.task_name)
        self.notebook_graph.nodes[self.task_name] = NotebookNode(
            task_name=self.task_name,
            notebook=self.main_notebook,
            type="notebook",
            cluster=self.cluster
        )
        self.notebook_graph.edges[self.task_name] = []

    def _transform_notebook(self) -> NotebookGraph:
        """Transform the notebook to be run in the test cache."""
        self._add_config_tasks()
        self._add_main_notebook_cell()
        self._process_dependencies()
        return self.notebook_graph

    def _add_config_tasks(self) -> None:
        """Add configuration tasks to the notebook graph."""
        if self.config is not None:
            for task, notebook in self.config.create_task_notebooks().items():
                self.notebook_graph.nodes[task] = NotebookNode(
                    task_name=task,
                    notebook=notebook,
                    type="task",
                    cluster=self.cluster
                )
                self.notebook_graph.edges[self.task_name].append(task)
            
            self.main_notebook.add_cell(self.config.generate_dbutils_config())

    def _add_main_notebook_cell(self) -> None:
        """Add the main notebook execution cell."""
        self.main_notebook.add_cell(f"%run {self.notebook_path}")

    def _process_dependencies(self) -> None:
        """Process notebook dependencies and merge graphs."""
        for dependency in self.depends_on:
            self.notebook_graph.edges[self.task_name].append(dependency.task_name)
            dep_graph = dependency._transform_notebook()
            
            self.notebook_graph.nodes.update(dep_graph.nodes)
            self._merge_edges(dep_graph.edges)

    def _merge_edges(self, dep_edges: Dict[str, List[str]]) -> None:
        """Merge dependency edges into the main graph."""
        for task, edges in dep_edges.items():
            if task in self.notebook_graph.edges:
                # Remove duplicates while preserving order
                combined = self.notebook_graph.edges[task] + edges
                self.notebook_graph.edges[task] = list(dict.fromkeys(combined))
            else:
                self.notebook_graph.edges[task] = edges


class NotebookTest:
    """Handles notebook testing functionality."""
    
    
    def __init__(self, notebook: Optional[Notebook] = None, cluster_id: Optional[str] = None):
        self.notebook = notebook
        self.cluster_id = cluster_id
        self.global_config = GlobalConfigManager()

    def __call__(self, fn: Union[Callable[..., Any], Type[Any]]):
        self.fn = fn
        self._validate_inputs()
        self._initialize_cluster_id()
        self._initialize_paths()
        self._setup_environment()
        return self


    def _validate_inputs(self) -> None:
        """Validate input parameters."""
        if self.notebook is not None and not isinstance(self.notebook, Notebook):
            raise NotebookValidationError("notebook must be a Notebook instance")

    def _initialize_cluster_id(self) -> None:
        """Initialize cluster ID from config if not provided."""
        if self.cluster_id is None:
            self.cluster_id = self.global_config.CLUSTER_ID

    def _initialize_paths(self) -> None:
        """Initialize file paths for the test."""
        self.current_path = Path(get_notebook_path())
        self.is_test = '_test_cache' not in self.current_path.parts
        
        if self.is_test:
            self._setup_test_paths()
        else:
            self._setup_cache_paths()

    def _setup_test_paths(self) -> None:
        """Setup paths for test execution."""
        relative_path = self.current_path.relative_to(self.global_config.TEST_PATH)
        self.test_cache_path = self.global_config.TEST_CACHE_PATH / relative_path.parent / '_test_cache'
        self.notebook_dir = self.test_cache_path / self.current_path.name / 'test_type=notebook' / self.fn.__name__
        self.task_dir = self.notebook_dir / 'tasks' 

    def _setup_cache_paths(self) -> None:
        """Setup paths for cache execution."""
        cache_index = self.current_path.parts.index("_test_cache") + 1
        self.test_cache_path = Path(*self.current_path.parts[:cache_index])
        self.notebook_dir = self.current_path.parent
        self.task_dir = self.notebook_dir / 'tasks' / self.fn.__name__

    def _setup_environment(self) -> None:
        """Setup the test environment."""
        if self.is_test:
            self._create_directories()
            self._save_test_cache()

    def _create_directories(self) -> None:
        """Create necessary directories for test execution."""
        for directory in [self.test_cache_path, self.notebook_dir, self.task_dir]:
            directory.mkdir(exist_ok=True, parents=True)

    def _save_test_cache(self) -> None:
        """Save the test cache notebook and tasks."""
        if self.notebook is None:
            return
            
        notebook_graph = self.notebook._transform_notebook()
        main_node = notebook_graph.nodes[self.notebook.task_name]
        
        # Add test execution cells
        main_node.notebook.add_cell(f"%run {self.current_path}")
        main_node.notebook.add_cell(f"{self.fn.__name__}.run()")
        
        self._save_notebooks(notebook_graph)
        self._create_submission(notebook_graph)

    def _save_notebooks(self, notebook_graph: NotebookGraph) -> None:
        """Save all notebooks in the graph."""
        for task, node in notebook_graph.nodes.items():
            save_path = (
                self.notebook_dir / task 
                if node.type == "notebook" 
                else self.task_dir / task
            )
            node.notebook.save_notebook(save_path.as_posix())

    def _create_submission(self, notebook_graph: NotebookGraph) -> None:
        """Create job submission with tasks."""
        submission = submit_run(self.fn.__name__, self.cluster_id)
        
        for task, edges in notebook_graph.edges.items():
            node = notebook_graph.nodes[task]
            task_path = (
                self.task_dir / task 
                if node.type == "task" 
                else self.notebook_dir / task
            )
            
            submission.add_task(
                task_key=task,
                notebook_path=task_path.as_posix(),
                cluster_id=node.cluster or self.cluster_id,
                depend_on=edges if edges else None,
                params={"trigger_run": "true"}
            )
        self.submission = submission
        logger.info(f"Created submission: {submission.as_dict()}")

    def run(self, debug: bool = False) -> None:
        """Run the notebook test."""
        trigger_run = get_param("trigger_run")
        
        if trigger_run is not None and trigger_run != "true":
            raise ValueError("Invalid trigger_run parameter")

        if debug and trigger_run is None:
            self._run_debug_mode()
        elif debug and trigger_run is not None:
            logger.warning("Unable to execute debug in triggered mode")
        elif not debug and trigger_run is not None:
            self._run_test_execution()
        else:
            logger.warning("Unable to execute standalone test without debug mode. Set debug=True")

    def _run_debug_mode(self) -> None:
        """Run in debug mode."""
        run = self.submission.run()
        logger.info(f"Test execution triggered: {run.run_id}")

    def _run_test_execution(self) -> None:
        """Execute the actual test function."""
        try:
            self.fn()
        except AssertionError:
            logger.error("Test failed due to assertion error")
        except Exception as e:
            logger.error(f"Test failed with exception: {e}")


class NotebookTestRunner:
    """Runs multiple notebook tests."""
    
    def __init__(self, test_path: str):
        self._validate_test_path(test_path)
        self._initialize_config(test_path)
        self._setup_paths()
        self._discover_tests()

    def _validate_test_path(self, test_path: str) -> None:
        """Validate that the test path exists."""
        if not Path(test_path).exists():
            raise NotebookValidationError(f"Test path does not exist: {test_path}")

    def _initialize_config(self, test_path: str) -> None:
        """Initialize global configuration."""
        self.global_config = GlobalConfigManager()
        self.global_config._load_config_from_test_path(test_path=test_path)
        self.cluster_id = self.global_config.CLUSTER_ID

    def _setup_paths(self) -> None:
        """Setup test and cache paths."""
        self.test_path = Path(self.global_config.TEST_PATH)
        self.test_cache_path = Path(self.global_config.TEST_CACHE_PATH)

    def _discover_tests(self) -> None:
        """Discover test notebooks and cached tests."""
        self.tests = [
            f for f in self.test_path.rglob("*")
            if is_notebook(f.as_posix()) and '_test_cache' not in f.parts
        ]
        
        self.test_cache = [
            f for f in self.test_cache_path.rglob("*")
            if ('test_type=notebook' in f.parts and 
                'tasks' not in f.parts and 
                is_notebook(f.as_posix()))
        ]

    def run(self) -> List[Any]:
        """Run all discovered tests."""
        logger.info(f"Running {len(self.tests)} test notebooks")
        
        # Run original test notebooks
        for test_notebook in self.tests:
            notebook_path = test_notebook.as_posix().split(".")[0]
            run_notebook(notebook_path, params={"trigger_run": "true"})

        logger.info(f"Found {len(self.test_cache)} cached tests")
        
        # Run cached test submissions
        runs = []
        for cached_test in self.test_cache:
            submission = self._create_cached_test_submission(cached_test)
            runs.append(submission.run())
        
        return runs

    def _create_cached_test_submission(self, cached_test: Path) -> Any:
        """Create submission for a cached test."""
        test_name = cached_test.name.split(".")[0]
        submission = submit_run(test_name, self.cluster_id)
        
        # Add task submissions
        tasks_dir = cached_test.parent / 'tasks' / test_name
        for task_path in tasks_dir.iterdir():
            task_name = task_path.name.split(".")[0]
            submission.add_task(
                task_name,
                task_path.as_posix().split(".")[0],
                params={"trigger_run": "true"}
            )
        
        # Add main task
        submission.add_task(
            f"{test_name}_task",
            cached_test.as_posix().split(".")[0],
            params={"trigger_run": "true"}
        )
        
        return submission