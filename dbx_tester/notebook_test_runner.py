import os
import inspect
import json
import uuid
from databricks.sdk.runtime import *
from global_config import GlobalConfig

from utils.databricks_api import (
    get_notebook_path,
    encode_notebook,
    create_notebook,
    create_cell,
    is_notebook,
    run_notebook
)

class NotebookTestRunner:
    def __init__(self):
        self.cfg = GlobalConfig()
        self.cfg.TEST_PATH("/".join(get_notebook_path().split("/")[:-1].append("tests")))        
        self.cfg.TEST_CACHE_PATH("/".join(get_notebook_path().split("/")[:-1].append("_test_cache")))


    def _prepare_combined_test_notebook(self):
        tests_dir = self.cfg.TEST_PATH
        cache_dir = self.cfg.TEST_CACHE_PATH

        if not os.path.exists(tests_dir):
            os.makedirs(tests_dir, exist_ok=True)

        if not os.path.exists(cache_dir):
            print(f"Creating test cache directory at {cache_dir}")
            os.mkdir(cache_dir)

        test_notebook_paths = self._collect_test_notebooks(tests_dir)

        combined_notebook = create_notebook("_imports")
        combined_cells = [create_cell(f"dbutils.notebook.run('{notebook_path}', 0)") for notebook_path in test_notebook_paths]
        combined_notebook['cells'] = combined_cells

        encode_notebook(os.path.join(cache_dir, "_imports"), combined_notebook)
        run_notebook(os.path.join(cache_dir, "_imports"))

    def _run_tests(self):
        print("Running tests is not implemented yet.")

    @classmethod
    def run_all(self):
        self._prepare_combined_test_notebook()
        self._run_tests()

    @staticmethod
    def _collect_test_notebooks(directory):
        test_notebooks = []
        for entry in os.listdir(directory):
            full_path = os.path.join(directory, entry)
            if os.path.isdir(full_path):
                test_notebooks += NotebookTestRunner._collect_test_notebooks(full_path)
            elif is_notebook(full_path):
                test_notebooks.append(full_path)
            else:
                print(f"Ignoring non-test file: {entry}")
        return test_notebooks