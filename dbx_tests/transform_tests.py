import inspect
import os

def do_magic_run(func, config = None, task_var = None):
    #Creating Empty notebook dict
    cells = []


    #Handle dbutils config
    if config is not None:
        empty_cell = create_cell(config.dbutils_config())
        cells.append(empty_cell)


    #Get the notebook
    func_code = inspect.getsource(func).split('\n', 1)[1]
    func_name = func.__name__
    notebook_name = func_name
 
    notebook_path = "/Workspace"+ dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() + f"/{func_name}"

    repo_path = os.path.join(REPO_PATH , os.path.relpath(notebook_path, TESTS_PATH)).split('/')

    repo_path[-1] = repo_path[-1].removeprefix("test_")

    empty_cell = create_cell(f'%run {"/".join(repo_path)}')
    cells.append(empty_cell)

    #Add the function
    empty_cell = create_cell(func_code)
    cells.append(empty_cell)

    #Execute the function 
    empty_cell = create_cell(f"{func_name}()")
    cells.append(empty_cell)

    empty_notebook_dict = create_notebook(func_name)
    empty_notebook_dict['cells'] = cells

    w = get_workspace_client()
    transformed_notebook_path = os.path.join(INTERNAL_PATH+"/tests" , os.path.relpath(notebook_path, TESTS_PATH))
    encode_notebook(w, transformed_notebook_path, empty_notebook_dict)

    #Handle task var
    if task_var is not None:
        notebooks = task_var.create_tasks()
        for task_name, notebook in notebooks.items():
            print(task_name, notebook)

    return lambda *args, **kwargs: print("Test function will be compiled and executed by THANOS")