from dbx_tester.global_config import GlobalConfig
from dbx_tester.utils.databricks_api import get_notebook_path
from pathlib import Path

class notebook_test():
    def __init__(self, fn, path, config):
        self.fn = fn
        self.path = path
        self.config = config
        self.global_config = GlobalConfig()

        self.notebook_dir = None
        self.task_dir = None

        self._create_files_and_folders()
        self._transform_notebook()

        return self

    def _create_files_and_folders(self):
        """
        This function will create the files and folders needed for the test cache
        """
        current_path = Path(get_notebook_path())

        if self.global_config.TEST_CACHE_PATH is None:
            test_cache_path = current_path.parent / '_notebook_test_cache'
        else: 
            test_cache_path = self.global_config.TEST_CACHE_PATH /current_path.relative_to(self.global_config.TEST_PATH) / '_notebook_test_cache'
        
        test_cache_path.mkdir(exist_ok=True)

        self.notebook_dir = test_cache_path / current_path.name
        self.notebook_dir.mkdir(exist_ok=True)

        self.task_dir = self.notebook_dir / 'tasks'
        self.notebook_dir.mkdir(exist_ok=True)

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
