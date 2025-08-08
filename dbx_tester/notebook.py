from dbx_tester.global_config import GlobalConfig
from dbx_tester.utils.databricks_api import *


class notebook_test():
    def __init__(self, fn, path, config):
        self.fn = fn
        self.path = path
        self.config = config
        self.global_config = GlobalConfig()


        self._transform_notebook()

        pass

    def _transform_notebook(self):
        """
        This function will transform the notebook to be run in the test cache
        """
        pass


class notebook_testrunner():
    def __init__(self):
        pass

    def _identify_notebooks(self):
        pass

    def _run_notebooks(self):
        pass

    def _identify_tests(self):
        pass

    def _run_tests(self):
        pass

    def run(self):
        pass
