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

class JobTestProcessError(Exception):
    pass

class JobTestState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELED = "CANCELED"

class JobTrigger(Enum):
    ON_DEMAND = "ON_DEMAND"
    WAIT = "WAIT"

@dataclass
class JobTestGraph:
    job_index: Dict[int, Job] = field(default_factory=dict)
    entry_point: Set[int] = field(default_factory=set)
    job_flow: Dict[int, Set[int]] = field(default_factory=dict)
    test_job: submit_run = None



@dataclass
class JobTestProcess:
    test_graph: JobTestGraph = None
    state: JobTestState = JobTestState.PENDING
    current_jobs: Set[int] = field(default_factory=set)
    runs: Dict[int,JobRunner] = field(default_factory=dict)
    logs: Dict[int,str] = field(default_factory=dict)



@dataclass(frozen=True)
class Job:
    name: str = None
    job_id: int = None
    config: JobConfigManager = None
    depends_on: List[Job] = field(default_factory=list)
    trigger: JobTrigger = JobTrigger.ON_DEMAND
    global_config: GlobalConfig = GlobalConfig()

    def __post_init__(self):
        self._validate_inputs()
        self._check_if_job_exists()

    def _validate_inputs(self):
        if self.name is None and self.job_id is None:
            raise ValueError("Either job name or job id must be provided")
        elif self.trigger not in JobTrigger:
            raise ValueError("Invalid trigger type")
        elif len(self.depends_on) == 0 and self.trigger == JobTrigger.WAIT:
            raise ValueError("Job with no dependencies cannot have WAIT trigger")
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

class JobTestProcessManager:
    def __init__(self, processs:JobTestProcess):
        self.processes: JobTestProcess = processs

    def _run_job(self, index):
        run = JobRunner(self.processes.test_graph.job_index[index].job_id, self.processes.test_graph.job_index[index].config).run()
        self.processes.current_jobs.add(index)
        self.processes.runs.update({index: run})
        self.processes.logs.update({index: run.get_run_status()})
    
    def _update_status(self, index):
        run = self.processes.runs[index]
        self.processes.logs.update({index: run.get_run_status()})

    def _init_process(self) -> None:
        for i in self.processes.test_graph.entry_point:
            self._run_job(i)
        self.processes.state = JobTestState.RUNNING
    
    def _stop_process(self) -> None:
        for i in self.processes.current_jobs:
            run = self.processes.runs[i]
            if run.get_run_status() == 'RUNNING':
                run.cancel_run()
                self.processes.logs.update({i: 'CANCELED'})
        self.processes.state = JobTestState.CANCELED
    
    def _check_and_update_current_state(self):
        for i in self.processes.current_jobs:
            run = self.processes.runs[i]
            self.processes.logs.update({i: run.get_run_status()})
    
    def _check_for_failure(self):
        for i in self.processes.current_jobs:
            if self.processes.logs[i] == 'FAILED':
                    self.processes.state = JobTestState.FAILED
                    return

    def _check_for_next_run(self):
        if self.processes.state != JobTestState.FAILED:
            return
        for i in self.processes.current_jobs:
            if self.processes.logs[i] != 'SUCCESS':
                continue
            next_jobs = self.processes.test_graph.job_flow.get(i, set())
            for next_job in next_jobs:
                if next_job not in self.processes.current_jobs:
                    self._run_job(next_job)
    
    def init(self):
        self._init_process()
        pass

    def state(self):
        return self.processes.state

    def monitor(self):
        self._check_and_update_current_state()
        self._check_for_failure()
        if self.processes.state != JobTestState.FAILED:
            self._stop_process()
            return
        self._check_for_next_run()

    def stop(self):
        self._stop_process()
        pass

class JobTest():
    def __init__(self, fn, job: Job):
        self.fn = fn
        self.job = job
        self.global_config = GlobalConfig()
        self.dep_graph = JobTestGraph()

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
        test_notebook.add_cell(f"%run {self.current_path.as_posix()}")
        test_notebook.add_cell(f"{self.fn.__name__}.run()")
        test_notebook.save_notebook((self.notebook_dir / f"{notebook_name}").as_posix())

    def _build_test_job(self):

        test_job_name = f"test_{self.fn.__name__}"
        self.dep_graph.test_job = submit_run(name=test_job_name, cluster_id=None).as_dict()
        pass

    def _save_test(self):
        #TODO
        pass

    def run(self):
        #TODO
        pass


class JobTestRunner():
    def __init__(self, test_path: str = None):
        self.global_config = GlobalConfig()

        self.test_path = Path(self.global_config.TEST_PATH)  
        self.test_cache_path = Path(self.global_config.TEST_CACHE_PATH)
        pass

    def _check_test_path(self):
        if not self.test_path.exists() or not self.test_path.is_dir():
            raise FileNotFoundError(f"Test path does not exist or is not a directory: {self.test_path}")
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
