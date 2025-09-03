from dbx_tester.utils.databricks_api import create_notebook
from pydantic import BaseModel
from typing import Any

class Widget(BaseModel):
    """Represents a widget with a key and value.

    Attributes:
        key (str): The key of the widget.
        value (str): The value of the widget.
    """
    key: str
    value: str

class TaskValue(BaseModel):
    """Represents a task value with a task key, key, and value.

    Attributes:
        taskKey (str): The task key associated with the value.
        key (str): The key of the task value.
        value (str): The value of the task.
    """
    taskKey: str
    key: str
    value: str

class RunParameter(BaseModel):
    """Represents a run parameter with a key and value.

    Attributes:
        key (str): The key of the run parameter.
        value (Any): The value of the run parameter.
    """
    key: str
    value: Any

class NotebookConfigManager():
    """Manages notebook configuration including widgets and task values."""
    
    def __init__(self):
        """Initializes the NotebookConfigManager with empty widgets and task values."""
        self.widgets = []
        self.task_values = []
    
    def add_widgets(self, key, value):
        """Adds a widget to the configuration.

        Args:
            key (str): The key of the widget.
            value (str): The value of the widget.

        Returns:
            NotebookConfigManager: The instance of the manager.
        """
        key = str(key)
        value = str(value)
        self.widgets.append(Widget(key=key, value=value))
        return self
    
    def add_task(self, taskKey, key, value):
        """Adds a task value to the configuration.

        Args:
            taskKey (str): The task key.
            key (str): The key of the task value.
            value (str): The value of the task.

        Returns:
            NotebookConfigManager: The instance of the manager.
        """
        self.task_values.append(TaskValue(taskKey=taskKey, key=key, value=value))
        return self
    
    def _dbutils_config(self):
        """Generates the DBUtils widget configuration script.

        Returns:
            str: The script for setting DBUtils widgets.
        """
        a = ""
        a += "##### Setting DBUtils Widgets with Default value #####\n"
        for widget in self.widgets:
            a += f"dbutils.widgets.text('{widget.key}', '{widget.value}')\n"
        return a
    
    def _create_tasks(self):
        """Creates tasks and returns a dictionary of notebooks.

        Returns:
            dict: A dictionary with task keys and their corresponding notebooks.
        """
        notebooks = dict()
        for task in self.task_values:
            if task.taskKey not in notebooks:
                notebooks[task.taskKey] = create_notebook(task.taskKey)
            notebooks[task.taskKey].add_cell(f"dbutils.jobs.taskValues.set(key = '{task.key}', value = '{task.value}')")
        return notebooks

class JobConfigManager():
    """Manages job configuration including run parameters."""
    
    def __init__(self):
        """Initializes the JobConfigManager with empty run parameters."""
        self.run_parameters = []

    def add_parameter(self, key, value):
        """Adds a run parameter to the configuration.

        Args:
            key (str): The key of the run parameter.
            value (Any): The value of the run parameter.
        """
        self.run_parameters.append(RunParameter(key=key, value=value))
    
    def _job_config(self):
        """Generates the job configuration dictionary.

        Returns:
            dict: A dictionary of run parameters.
        """
        return {i.key: i.value for i in self.run_parameters}