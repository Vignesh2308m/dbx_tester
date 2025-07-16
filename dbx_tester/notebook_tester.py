import os
import inspect
import json
import uuid
from databricks.sdk.runtime import *

from databricks_api import (
    get_workspace_client,
    get_notebook_path,
    encode_notebook,
    create_notebook,
    create_cell
)

class NotebookTestRunner:
    def __init__(self):
        pass

    def _prepare_combined_test_notebook(self):
        current_path = "/Workspace/" + "/".join(get_notebook_path().split("/")[:-1])
        tests_dir = os.path.join(current_path, "tests")
        cache_dir = os.path.join(current_path, "_test_cache")

        if not os.path.exists(tests_dir):
            os.makedirs(tests_dir, exist_ok=True)

        if not os.path.exists(cache_dir):
            print(f"Creating test cache directory at {cache_dir}")
            os.mkdir(cache_dir)

        test_notebook_paths = self._collect_test_notebooks(tests_dir)

        combined_notebook = create_notebook("_imports")
        combined_cells = [create_cell(f"dbutils.notebook.run('{notebook_path}', 0)") for notebook_path in test_notebook_paths]
        combined_notebook['cells'] = combined_cells

        workspace_client = get_workspace_client()
        encode_notebook(workspace_client, os.path.join(cache_dir, "_imports"), combined_notebook)

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
            elif entry.startswith("test_") and "." not in entry:
                test_notebooks.append(full_path)
            else:
                print(f"Ignoring non-test file: {entry}")
        return test_notebooks


def notebook_test(func, config=None):
    cells = []

    # Add dbutils config if provided
    if config is not None:
        cells.append(create_cell(config.dbutils_config()))

    func_code = inspect.getsource(func).split('\n', 1)[1]
    func_name = func.__name__
    notebook_name = func_name

    current_notebook_path = "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    new_notebook_path = os.path.join(current_notebook_path, func_name)

    relative_repo_path = os.path.join(REPO_PATH, os.path.relpath(new_notebook_path, TESTS_PATH)).split('/')
    relative_repo_path[-1] = relative_repo_path[-1].removeprefix("test_")
    cells.append(create_cell(f"%run {'/'.join(relative_repo_path)}"))

    cells.append(create_cell(func_code))
    cells.append(create_cell(f"{func_name}()"))

    test_notebook = create_notebook(func_name)
    test_notebook['cells'] = cells

    workspace_client = get_workspace_client()
    final_test_path = os.path.join(INTERNAL_PATH + "/tests", os.path.relpath(new_notebook_path, TESTS_PATH))
    encode_notebook(workspace_client, final_test_path, test_notebook)

    if task_var is not None:
        for task_name, notebook in task_var.create_tasks().items():
            print(task_name, notebook)

    return lambda *args, **kwargs: print("Test function will be compiled and executed by the framework.")
