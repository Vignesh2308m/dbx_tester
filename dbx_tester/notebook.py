from dbx_tester.global_config import GlobalConfig
from dbx_tester.utils.databricks_api import get_notebook_path


class notebook_test():
    def __init__(self, fn, path, config):
        self.fn = fn
        self.path = path
        self.config = config
        self.global_config = GlobalConfig()


        self._transform_notebook()

        pass

    def _create_files_and_folders(self):
        """
        This function will create the files and folders needed for the test cache
        """
        current_path = get_notebook_path()

        if self.global_config.TEST_CACHE_PATH:
            pass

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
