from dbx_tester.global_config import GlobalConfig
from dbx_tester.utils.databricks_api import *
from pathlib import Path
import json


class JobNotFoundError(ValueError):
    pass

class Job:
    def __init__(self, name = None, job_id = None , config = {}, depends_on = None):
        self.name = name
        self.job_id = job_id
        self.config = config
        self.depends_on = depends_on if depends_on else []
        self.global_config = GlobalConfig()

        self._validate_inputs()
        self._check_if_job_exists()
        pass

    def _validate_inputs(self):
        pass

    def _check_if_job_exists(self):
        pass

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
