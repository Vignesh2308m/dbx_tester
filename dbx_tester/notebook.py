from dbx_tester.global_config import GlobalConfigManager
from dbx_tester.config_manager import NotebookConfigManager
from dbx_tester.utils.databricks_api import get_notebook_path, create_cell, create_notebook, submit_run, is_notebook, run_notebook
from pathlib import Path
from collections.abc import Callable
from typing import Type, Any

class notebook_test():
    def __init__(self, fn, notebook_path=None, config=None, cluster_id = None):
        self.fn:Callable[..., Any] | Type[Any] = fn
        self.notebook_path = notebook_path
        self.config:NotebookConfigManager = config
        self.global_config = GlobalConfigManager()
        
        if config is not None and not isinstance(config, NotebookConfigManager):
            raise ValueError("INVALID TEST CASE CONFIG: Add a Notebook config Manager instance")
        
        if self.notebook_path is not None and (
            not Path(self.notebook_path).exists() and 
            not (self.global_config.REPO_PATH is None or
                 (Path(self.global_config.REPO_PATH) / Path(self.notebook_path)).exists())):
            raise ValueError(f"INVALID NOTEBOOK PATH: {self.notebook_path} is invalid or not exists")
        
        if self.notebook_path is not None and (
            not Path(self.notebook_path).exists() and 
            self.global_config.REPO_PATH is not None and
            (Path(self.global_config.REPO_PATH) / Path(self.notebook_path)).exists()):
                self.notebook_path = (Path(self.global_config.REPO_PATH) / Path(self.notebook_path)).as_posix()
            
        if cluster_id is None:
            self.cluster_id = self.global_config.CLUSTER_ID
        else:
            self.cluster_id = cluster_id

        self.current_path = Path(get_notebook_path())
        self.is_test = '_test_cache' not in self.current_path.parts

        if self.is_test:
            self.test_cache_path = self.global_config.TEST_CACHE_PATH / self.current_path.relative_to(self.global_config.TEST_PATH).parent / '_test_cache'
            self.notebook_dir = self.test_cache_path / self.current_path.name / 'test_type=notebook'
            self.task_dir = self.notebook_dir / 'tasks'

            self._create_files_and_folders()
            self._transform_notebook()
        else:
            self.test_cache_path = Path(*self.current_path.parts[:self.current_path.parts.index("_test_cache")+1])
            self.notebook_dir = self.current_path.parent
            self.task_dir = self.notebook_dir / 'tasks'

    def _create_files_and_folders(self):
        """
        This function will create the files and folders needed for the test cache
        """

        self.test_cache_path.mkdir(exist_ok=True, parents=True)
        self.notebook_dir.mkdir(exist_ok=True, parents=True)
        self.task_dir.mkdir(exist_ok=True, parents=True)

        pass

    def _transform_notebook(self):
        """
        This function will transform the notebook to be run in the test cache
        """
        test_notebook = create_notebook(self.fn.__name__)

        if self.config is not None:
            for task, notebook in self.config._create_tasks().items():
                notebook.save_notebook(self.task_dir / self.fn.__name__ / task)
            
            test_notebook.add_cell(self.config._dbutils_config())


        if self.notebook_path is not None:
            test_notebook.add_cell(f"%run {self.notebook_path}")
        
        test_notebook.add_cell(f"%run {self.current_path}")

        test_notebook.add_cell(f"{self.fn.__name__}.run()")

        test_notebook.save_notebook((self.notebook_dir / self.fn.__name__).as_posix())

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
    def __init__(self, test_path):
        if not Path(test_path).exists():
            raise ValueError(f"INVALID TEST PATH: Test path not exists {test_path}")
        
        self.global_config = GlobalConfigManager()
        self.global_config._load_config_from_test_path(test_path=test_path)

        self.cluster_id = self.global_config.CLUSTER_ID
        self.test_path = Path(self.global_config.TEST_PATH)  
        self.test_cache_path = Path(self.global_config.TEST_CACHE_PATH)

        self.tests = [f for f in self.test_path.rglob("*") if is_notebook(f) and '_test_cache' not in f.parts]
        self.test_cache = [f for f in self.test_cache_path.rglob("*") if '_test_cache' in f.parts and 'tasks' not in f.parts and is_notebook(f)]

    def run(self):  
        runs = []

        for i in self.tests:
            run_notebook(i)

        for i in self.test_cache:
            s = submit_run(i.name, self.cluster_id)

            for path in (i.parent /'tasks'/ i.name).iterdir():
                s.add_task(path.name, path)
            s.add_task(i.name+'_task',i)
            runs.append(s.run())
           
        pass