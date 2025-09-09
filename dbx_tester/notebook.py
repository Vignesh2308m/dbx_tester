from __future__ import annotations

from dbx_tester.global_config import GlobalConfigManager
from dbx_tester.config_manager import NotebookConfigManager
from dbx_tester.utils.databricks_api import get_notebook_path, create_notebook, submit_run, is_notebook, run_notebook
from dbx_tester.utils.databricks_dbutils import get_param


from pathlib import Path
from collections.abc import Callable
from typing import Type, Any, List, Dict, Literal
from datetime import datetime
from dataclasses import dataclass, field


@dataclass
class NotebookNode:
    task_name: str
    notebook: create_notebook
    type: Literal["notebook", "task"] = "notebook"
    cluster: str = None

@dataclass
class NotebookGraph:
    nodes : Dict[str, NotebookNode] = field(default_factory=dict)
    edges: Dict[str, List[str]] = field(default_factory=dict)


class notebook():
    def __init__(self, notebook_path:str, task_name:str = None ,config:NotebookConfigManager = None, cluster = None, depends_on:notebook|List[notebook]=None):
        self.notebook_path = notebook_path
        self.task_name = task_name
        self.config = config
        self.cluster = cluster
        self.depends_on = depends_on if depends_on is not None else []
        self.notebook_graph = NotebookGraph()
        self.global_config = GlobalConfigManager()
        self.global_config._load_config()

        if (
            self.notebook_path is not None and
            not Path(self.notebook_path).exists() and
            not (
                self.global_config.REPO_PATH is None or
                (Path(self.global_config.REPO_PATH) / Path(self.notebook_path)).exists()
            )
        ):
            raise ValueError(f"INVALID NOTEBOOK PATH: {self.notebook_path} is invalid or not exists")
        
        if (
            self.notebook_path is not None and
            not Path(self.notebook_path).exists() and 
            self.global_config.REPO_PATH is not None
        ):
            self.notebook_path = (Path(self.global_config.REPO_PATH) / Path(self.notebook_path)).as_posix()


        if self.task_name is not None and (not isinstance(self.task_name, str) or self.task_name.strip() == ""):
            raise ValueError("INVALID TASK NAME: Add a valid task name")

        if config is not None and not isinstance(config, NotebookConfigManager):
            raise ValueError("INVALID NOTEBOOK CONFIG: Add a NotebookConfigManager instance")
        
        if depends_on is not None:
            if isinstance(depends_on, notebook):
                self.depends_on = [depends_on]
            elif isinstance(depends_on, list):
                for i in depends_on:
                    if not isinstance(i, notebook):
                        raise ValueError("INVALID DEPENDS ON: Add a notebook instance or list of notebook instances")
            else:
                raise ValueError("INVALID DEPENDS ON: Add a notebook instance or list of notebook instances")
        
        if self.task_name is None:
            self.task_name = f"{Path(self.notebook_path).stem}_task_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        self.main_notebook = create_notebook(self.task_name)
        self.notebook_graph.nodes[self.task_name] = NotebookNode(task_name=self.task_name, 
                                                                 notebook=self.main_notebook, 
                                                                 type="notebook", 
                                                                 cluster=self.cluster)
        self.notebook_graph.edges[self.task_name] = []

    def _transform_notebook(self):
        """
        Transforms the notebook to be run in the test cache.

        Returns:
            tuple: (main_notebook, dep_notebooks)
                main_notebook: The primary notebook object to be run.
                dep_notebooks: List of dependent notebook objects.
        """

        if self.config is not None:
            for task, notebook in self.config._create_tasks().items():
                self.notebook_graph.nodes[task] = NotebookNode(task_name=task, 
                                                               notebook=notebook, 
                                                               type="task", 
                                                               cluster=self.cluster)
                self.notebook_graph.edges[self.task_name].append(task)

            self.main_notebook.add_cell(self.config._dbutils_config())

        self.main_notebook.add_cell(f"%run {self.notebook_path}")

        for i in self.depends_on:
            self.notebook_graph.edges[self.task_name].append(i.task_name)
            notebook_graph = i._transform_notebook()
            self.notebook_graph.nodes.update(notebook_graph.nodes)
            for task, edges in notebook_graph.edges.items():
                if task in self.notebook_graph.edges:
                    self.notebook_graph.edges[task] = list(set(self.notebook_graph.edges[task] + edges))
                else:
                    self.notebook_graph.edges[task] = edges

        return self.notebook_graph
    


class notebook_test():
    def __init__(self, fn, notebooks: notebook|list[notebook] = None, cluster_id = None):
        """
        Initializes a notebook test.

        Args:
            fn (Callable[..., Any] | Type[Any]):
                The function or type to test.
            notebooks (notebook | list[notebook], optional): 
                The notebook or list of notebooks to test.
            cluster_id (str, optional): 
                Cluster ID for the test.
        Results:
            notebook_test object
        """

        self.fn:Callable[..., Any] | Type[Any] = fn
        self.notebooks = notebooks if isinstance(notebooks, list) else [notebooks]
        self.cluster_id = cluster_id

        self.global_config = GlobalConfigManager()
        
            
        if cluster_id is None:
            self.cluster_id = self.global_config.CLUSTER_ID
        else:
            self.cluster_id = cluster_id

        self.current_path = Path(get_notebook_path())
        self.is_test = '_test_cache' not in self.current_path.parts

        if self.is_test:
            self.test_cache_path = self.global_config.TEST_CACHE_PATH / self.current_path.relative_to(self.global_config.TEST_PATH).parent / '_test_cache'
            self.notebook_dir = self.test_cache_path / self.current_path.name / 'test_type=notebook'
            self.task_dir = self.notebook_dir / 'tasks' / self.fn.__name__

            self._create_files_and_folders()
            self._transform_notebook()
        else:
            self.test_cache_path = Path(*self.current_path.parts[:self.current_path.parts.index("_test_cache")+1])
            self.notebook_dir = self.current_path.parent
            self.task_dir = self.notebook_dir / 'tasks' / self.fn.__name__

    def _create_files_and_folders(self):
        """
        Creates the files and folders needed for the test cache.
        """
        self.test_cache_path.mkdir(exist_ok=True, parents=True)
        self.notebook_dir.mkdir(exist_ok=True, parents=True)
        self.task_dir.mkdir(exist_ok=True, parents=True)
    
    def _save_test_cache(self):

        """
        Saves the test cache notebook and tasks.
        """
        notebook_graph = self._transform_notebook()

        #TODO

    

    def run(self, debug=False):
        """
        Runs the notebook test.

        Args:
            debug (bool): If True, run in debug mode.
        """
        trigger_run = get_param("trigger_run")
        if trigger_run is not None and trigger_run != "true":
            raise ValueError("Invalid trigger run param")

        if debug and trigger_run is None:
            s = submit_run(self.fn.__name__, self.cluster_id)

            for path in self.task_dir.iterdir():
                s.add_task(path.name, path.as_posix(),params={"trigger_run": "true"})
            
            s.add_task(self.fn.__name__+'_task' , (self.notebook_dir / self.fn.__name__).as_posix(),params={"trigger_run": "true"})
            s.run()

        elif debug and trigger_run is not None:
            print("Unable to execute debug in triggered mode")

        elif not debug and trigger_run is not None:
            try:
                self.fn()
            except AssertionError as err:
                print("Failed due to assertion Error")
            except Exception as err:
                print("Test Failed")

        else:
            print("Unable to execute standalone test without debug mode. try set debug = True")
        

class notebook_testrunner():
    def __init__(self, test_path):
        """
        Initializes a notebook test runner.

        Args:
            test_path (str): Path to the test directory.
        Results:
            notebook_testrunner object
        """
        if not Path(test_path).exists():
            raise ValueError(f"INVALID TEST PATH: Test path not exists {test_path}")
        
        self.global_config = GlobalConfigManager()
        self.global_config._load_config_from_test_path(test_path=test_path)

        self.cluster_id = self.global_config.CLUSTER_ID
        
        self.test_path = Path(self.global_config.TEST_PATH)  
        self.test_cache_path = Path(self.global_config.TEST_CACHE_PATH)

        self.tests = [f for f in self.test_path.rglob("*") if is_notebook(f.as_posix()) and '_test_cache' not in f.parts]
        self.test_cache = [f for f in self.test_cache_path.rglob("*") if 'test_type=notebook' in f.parts and 'tasks' not in f.parts and is_notebook(f.as_posix())]

    def run(self):  
        """
        Runs all notebook tests.
        """
        runs = []

        for i in self.tests:
            run_notebook(i.as_posix().split(".")[0], params={"trigger_run": "true"})
        
        print(self.test_cache)

        for i in self.test_cache:
            s = submit_run(i.name.split(".")[0], self.cluster_id)

            for path in (i.parent /'tasks'/ i.name.split(".")[0]).iterdir():
                s.add_task(path.name.split(".")[0], path.as_posix().split(".")[0], params={"trigger_run": "true"})
            s.add_task(i.name.split(".")[0]+'_task',i.as_posix().split(".")[0], params={"trigger_run": "true"})
            runs.append(s.run())