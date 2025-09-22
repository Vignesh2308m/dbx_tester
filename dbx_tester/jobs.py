from __future__ import annotations

from dbx_tester.global_config import GlobalConfig
from dbx_tester.utils.databricks_api import *
from dbx_tester.config_manager import JobConfigManager

from typing import List, Dict, Set
from enum import Enum
from pathlib import Path
from dataclasses import dataclass, field
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JobNotFoundError(ValueError):
    pass

class JobTestError(ValueError):
    pass

@dataclass
class TestGraph:
    job_index: Dict[int, Job] = field(default_factory=dict)
    entry_point: Set[int] = field(default_factory=set)
    job_flow: Dict[int, Set[int]] = field(default_factory=dict)
    test_job: submit_run = None

@dataclass(frozen=True)
class Job:
    name: str = None
    job_id: int = None
    config: JobConfigManager = None
    depends_on: List[Job] = field(default_factory=list)
    global_config: GlobalConfig = GlobalConfig()

    def __post_init__(self):
        self._validate_inputs()
        self._check_if_job_exists()

    def _validate_inputs(self):
        if self.name is None and self.job_id is None:
            raise ValueError("Either job name or job id must be provided")
        elif self.name is not None and self.job_id is not None:
            raise ValueError("Only one of job name or job id must be provided")
        elif self.name is not None and not isinstance(self.name, str):
            raise ValueError("Job name must be a string")
        elif self.job_id is not None and not isinstance(self.job_id, int):
            raise ValueError("Job id must be an integer")
        elif not isinstance(self.config, JobConfigManager):
            raise ValueError("Config must be a JobConfigManager object")
        elif not isinstance(self.depends_on, (Job, list)):
            raise ValueError("Depends on must be a Job object or a list of Job objects")
        elif isinstance(self.depends_on, list):
            for i in self.depends_on:
                if not isinstance(i, Job):
                    raise ValueError("Depends on must be a list of Job objects")
        else:
            return True

    def _check_if_job_exists(self):
        if not is_job(name=self.name, job_id=self.job_id):
            raise JobNotFoundError(f"Job not found: Job with name {self.name} or id {self.job_id} not found")
        else:
            object.__setattr__(self, 'job_id', get_job_id(name=self.name, job_id=self.job_id))

class JobTest():
    def __init__(self, fn, job: Job):
        self.fn = fn
        self.job = job
        self.global_config = GlobalConfig()
        self.dep_graph = TestGraph()

        self._initialize_paths()
        self._build_dep_graph()
        self._build_test_notebook()
        pass

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

    def _setup_cache_paths(self) -> None:
        """Setup paths for cache execution."""
        cache_index = self.current_path.parts.index("_test_cache") + 1
        self.test_cache_path = Path(*self.current_path.parts[:cache_index])
        self.notebook_dir = self.current_path.parent

    def _build_dep_graph(self):

        visited = set()
        
        stack = [{0:self.job}]

        while stack:
            index, job = stack.pop()
            if job in visited:
                raise JobTestError("Circular dependency detected")
            visited.add(job)
            if index not in self.dep_graph.job_flow:
                self.dep_graph.job_flow[index] = set()
            if index > 0:
                self.dep_graph.job_flow[index].add(index - 1)
            self.dep_graph.job_index[index] = job
            if len(job.depends_on) == 0:
                self.dep_graph.entry_point.add(index)
            else:
                for dep in job.depends_on:
                    dep_index = index + 1
                    stack.append({dep_index: dep})
    
    def _build_test_notebook(self):

        notebook_name = f"test_{self.fn.__name__}"

        test_notebook = notebook_builder(notebook_name)

        test_notebook.add_cell(f"%run {get_notebook_path()}")

        test_notebook.add_cell(f"{self.fn.__name__}.run()")

        test_notebook.save(self.notebook_dir / f"{notebook_name}")

    def _build_test_job(self):

        test_job_name = f"test_{self.fn.__name__}"

        self.dep_graph.test_job = submit_run(name=test_job_name, cluster_id=None)

        #TODO
        pass

    def _save_test_artifacts(self):
        #TODO
        pass

    def run(self):
        #TODO
        pass


class JobTestRunner():
    def __init__(self):
        self.global_config = GlobalConfig()

        self.test_path = Path(self.global_config.TEST_PATH)  
        self.test_cache_path = Path(self.global_config.TEST_CACHE_PATH)
        pass

    def _identify_job_tests(self):
        self.tests = [f for f in self.test_path.rglob("*") if is_notebook(f) and '_test_cache' not in f.parts]
        pass

    def _run_job_tests(self):
        for i in self.tests:
            run_notebook(i)
        pass

    def _identify_job_cache(self):
        self.test_cache = [f for f in self.test_cache_path.rglob("*") if '_test_cache' in f.parts and 'tasks' not in f.parts and is_notebook(f)]
        pass

    def _run_job_cache(self):
        #TODO
        pass

    def run(self):
        pass
