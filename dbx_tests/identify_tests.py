import os
import json
import uuid
import inspect
from databricks.sdk.runtime import *

from workspace_api import get_workspace_client, get_notebook_path, encode_notebook, create_notebook, create_cell



def walk_test_dir(test_path):
    path = []
    for i in os.listdir(test_path):
        if os.path.isdir(test_path + "/" + i):
            path += walk_test_dir(test_path + "/" + i)
        elif i.startswith("test_") and "." not in i:
            path.append(test_path + "/" + i)
        else:
            print(f"Ignoring non-test notebook/file - {i}")
        
    return path

def identify_test():

    parent_path = "/Workspace/"+"/".join(get_notebook_path().split("/")[:-1])

    test_path = parent_path + "/tests"
    if not os.path.exists(test_path):
        raise Exception(f"Couldn't find tests directory at {test_path}")

    test_cache_path = parent_path + "/_test_cache"
    if not os.path.exists(test_cache_path):
        print(f"Creating test cache directory at {test_cache_path}")
        os.mkdir(test_cache_path)


    paths = walk_test_dir(test_path)

    # Initializing empty notebook dict
    empty_notebook_dict = create_notebook("_imports")
    cells = []

    for i in paths:
        empty_cell = create_cell(f"dbutils.notebook.run('{i}', 0)")
        cells.append(empty_cell)
    
    empty_notebook_dict['cells'] = cells
    print("DEBUG: TILL THIS FINE")
    
    w = get_workspace_client()
    encode_notebook(w, test_cache_path + "/_imports", empty_notebook_dict)