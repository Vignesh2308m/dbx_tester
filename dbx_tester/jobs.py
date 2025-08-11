from dbx_tester.global_config import GlobalConfig
from dbx_tester.utils.databricks_api import *


class job_test():
    def __init__(self, fn, job_id, config):
        self.fn = fn
        self.job_id = job_id
        self.config = config
        self.global_config = GlobalConfig()


        self._transform_notebook()

        pass

    def _transform_notebook(self):
        """
        This function will transform the notebook to be run in the test cache
        """
        
        pass

class job_test_runner():
    def __init__(self):
        pass

    def _identify_job_tests(self):
        pass

    def _run_job_tests(self):
        pass

    def _identify_job_cache(self):
        pass

    def _run_job_cache(self):
        pass

    def run(self):
        pass
