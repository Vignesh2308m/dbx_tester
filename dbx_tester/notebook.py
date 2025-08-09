from dbx_tester.global_config import GlobalConfig
from dbx_tester.config_manager import ConfigManager
from dbx_tester.utils.databricks_api import get_notebook_path, create_cell, create_notebook
from pathlib import Path
from collections.abc import Callable
from typing import Type, Any

class notebook_test():
    def __init__(self, fn, path=None, config=None):
        self.fn:Callable[..., Any] | Type[Any] = fn
        self.path = path
        self.config:ConfigManager = config
        self.global_config = GlobalConfig()

        self.current_path = None
        self.notebook_dir = None
        self.task_dir = None

        self._create_files_and_folders()
        self._transform_notebook()


    def _create_files_and_folders(self):
        """
        This function will create the files and folders needed for the test cache
        """
        self.current_path = Path(get_notebook_path())

        if self.global_config.TEST_CACHE_PATH is None:
            test_cache_path = self.current_path.parent / '_notebook_test_cache'
        else: 
            test_cache_path = self.global_config.TEST_CACHE_PATH / self.current_path.relative_to(self.global_config.TEST_PATH) / '_notebook_test_cache'
        
        test_cache_path.mkdir(exist_ok=True)

        self.notebook_dir = test_cache_path / self.current_path.name
        self.notebook_dir.mkdir(exist_ok=True)

        self.task_dir = self.notebook_dir / 'tasks'
        self.task_dir.mkdir(exist_ok=True)

        pass

    def _transform_notebook(self):
        """
        This function will transform the notebook to be run in the test cache
        """
        test_notebook = create_notebook(self.fn.__name__)

        if self.config is not None:
            for task, notebook in self.config._create_tasks().items():
                notebook.save_notebook(self.task_dir / task)
            
            test_notebook + create_cell(self.config._dbutils_config())


        if self.path is not None:
            test_notebook + create_cell(f"%run {self.path}")
        
        test_notebook + create_cell(f"%run {self.current_path}")

        test_notebook + create_cell(f"{self.fn.__name__}().run()")

        pass

    def run(self):
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
