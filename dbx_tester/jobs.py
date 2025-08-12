from dbx_tester.global_config import GlobalConfig
from dbx_tester.utils.databricks_api import *
from pathlib import Path
import json



class job_test():
    def __init__(self, fn, job_id, config):
        self.fn = fn
        self.job_id = job_id
        self.config = config
        self.global_config = GlobalConfig()

        self.current_path = Path(get_notebook_path())
        self.is_test = '_job_test_cache' not in self.current_path.parts

        if self.is_test:
            self.test_cache_path = self.global_config.TEST_CACHE_PATH / self.current_path.relative_to(self.global_config.TEST_PATH).parent / '_notebook_test_cache'
            self.notebook_dir = self.test_cache_path / self.current_path.name
            self.config_dir = self.notebook_dir / 'config'
            
            self._create_files_and_folders()
            self._transform_notebook()
        else:
            self.test_cache_path = Path(*self.current_path.parts[:self.current_path.parts.index("_notebook_test_cache")+1])
            self.notebook_dir = self.current_path.parent
            self.config_dir = self.notebook_dir / 'config'

        pass

    def _create_files_and_folders(self):
        """
        This function will create the files and folders needed for the test cache
        """

        self.test_cache_path.mkdir(exist_ok=True, parents=True)
        self.notebook_dir.mkdir(exist_ok=True, parents=True)
        self.config_dir.mkdir(exist_ok=True, parents=True)

        pass

    def _transform_notebook(self):
        """
        This function will transform the notebook to be run in the test cache
        """
        test_notebook = create_notebook(self.fn.__name__)

        test_notebook + create_cell(f"%run {self.current_path}")

        test_notebook + create_cell(f"{self.fn.__name__}().run()")

        test_notebook.save_notebook(self.notebook_dir / self.fn.__name__)

        with open(self.config_dir / f"{self.fn.__name__}.json", "w") as f:
                json.dump(self.config, f)
                
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
