from dbx_tester.global_config import GlobalConfig
from dbx_tester.config_manager import ConfigManager
from dbx_tester.utils.databricks_api import get_notebook_path, create_cell, create_notebook, submit_run
from pathlib import Path
from collections.abc import Callable
from typing import Type, Any

class notebook_test():
    def __init__(self, fn, path=None, config=None, cluster_id = None):
        self.fn:Callable[..., Any] | Type[Any] = fn
        self.path = path
        self.config:ConfigManager = config
        self.global_config = GlobalConfig()
        if cluster_id is None:
            self.cluster_id = self.global_config.CLUSTER_ID
        else:
            self.cluster_id = cluster_id

        self.current_path = Path(get_notebook_path())
        self.is_test = '_notebook_test_cache' not in self.current_path.parts

        if self.is_test:
            self.test_cache_path = self.global_config.TEST_CACHE_PATH / self.current_path.relative_to(self.global_config.TEST_PATH).parent / '_notebook_test_cache'
            self.notebook_dir = self.test_cache_path / self.current_path.name
            self.task_dir = self.notebook_dir / 'tasks'

            self._create_files_and_folders()
            self._transform_notebook()
        else:
            self.test_cache_path = Path(*self.current_path.parts[:self.current_path.parts.index("_notebook_test_cache")+1])
            self.notebook_dir = self.current_path.parent
            self.task_dir = self.notebook_dir / 'tasks'

    def _create_files_and_folders(self):
        """
        This function will create the files and folders needed for the test cache
        """

        self.test_cache_path.mkdir(exist_ok=True)
        self.notebook_dir.mkdir(exist_ok=True)
        self.task_dir.mkdir(exist_ok=True)

        pass

    def _transform_notebook(self):
        """
        This function will transform the notebook to be run in the test cache
        """
        test_notebook = create_notebook(self.fn.__name__)

        if self.config is not None:
            for task, notebook in self.config._create_tasks().items():
                notebook.save_notebook(self.task_dir / self.fn.__name__ / task)
            
            test_notebook + create_cell(self.config._dbutils_config())


        if self.path is not None:
            test_notebook + create_cell(f"%run {self.path}")
        
        test_notebook + create_cell(f"%run {self.current_path}")

        test_notebook + create_cell(f"{self.fn.__name__}().run()")

        test_notebook.save_notebook(self.notebook_dir / self.fn.__name__)

        pass

    def run(self, debug=False):
        if debug:
            s = submit_run(self.fn.__name__, self.cluster_id)

            for path in (self.task_dir / self.fn.__name__).iterdir():
                s.add_task(path.name, path)
            
            s.add_task(self.fn.__name__+'_task',self.notebook_dir / self.fn.__name__)
            s.run()
            pass
        else:
            try:
                self.fn()
            except AssertionError as err:
                print("Failed due to assertion Error")
            except Exception as err:
                print("Try Debug mode")
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
