from __future__ import annotations

from dbx_tester.global_config import GlobalConfig
from dbx_tester.utils.databricks_api import *
from dbx_tester.config_manager import JobConfigManager

from typing import List
from pathlib import Path
import json


class JobNotFoundError(ValueError):
    pass

class Job:
    def __init__(self, name:str = None, job_id:int = None , config:JobConfigManager = None , depends_on:Job|List[Job] = None):
        self.name = name
        self.job_id = job_id
        self.config = config
        self.depends_on = depends_on if depends_on else []
        self.global_config = GlobalConfig()

        self._validate_inputs()
        self._check_if_job_exists()
        pass

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
            self.job_id = get_job_id(self.name) if self.job_id is None else self.job_id

class JobTest():
    def __init__(self, fn, job = None, job_id = None, config = {}):
        self.fn = fn
        self.job_id = job_id
        self.config = config
        self.global_config = GlobalConfig()
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
