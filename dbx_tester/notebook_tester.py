import os

from global_config import GlobalConfig
from config_manager import ConfigManager
from utils.databricks_api import encode_notebook, create_notebook, create_cell, get_notebook_path


def notebook_test(func, notebook_path=None, notebook_config=None, is_fullpath=False):
    cfg = GlobalConfig()

    if notebook_config is not None and not isinstance(notebook_config, ConfigManager):
        raise Exception("Please use appropriate config manager")
    
    if cfg.REPO_PATH is None and not is_fullpath:
        raise Exception("Please set the REPO_PATH using global_config")

    relative_test_path = os.path.relpath(get_notebook_path(), cfg.TEST_PATH)
    test_cache_path = os.path.join(cfg.TEST_CACHE_PATH, relative_test_path)

    if not os.path.exists(test_cache_path):
        os.mkdir(test_cache_path)
    if not os.path.exists(test_cache_path + "/tasks"):
        os.mkdir(test_cache_path + "/tasks")

    notebook_full_path = ""
    if is_fullpath:
        notebook_full_path = notebook_path
    else:
        notebook_full_path = os.path.join(cfg.REPO_PATH, notebook_path)
    
    cells = []
    if notebook_config is not None:
        cells.append(create_cell(notebook_config._dbutils_config))

        for taskKey, notebook in notebook_config._create_tasks.items():
            encode_notebook(test_cache_path+f"/tasks/{taskKey}", notebook)

    cells.append(create_cell(f"%run {notebook_full_path}"))    
    cells.append(create_cell(f"%run {get_notebook_path()}"))

    cells.append(create_cell(f"{func.__name__}()"))

    test_notebook = create_notebook(f"{func.__name__}")
    test_notebook["cells"] = cells

    encode_notebook(test_cache_path+f"/{func.__name__}", test_notebook)
    