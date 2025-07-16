from databricks_api import create_notebook, create_cell
from pydantic import BaseModel


class Widget(BaseModel):
    key : str
    value: str

class TaskValue(BaseModel):
    taskKey: str
    key : str
    value: str

class WidgetManager():
    def __init__(self):
        self.widgets = []
        self.task_values = []
    
    def add_widgets(self, key, value):
        key = str(key)
        value = str(value)
        self.widgets.append(Widget(key=key, value=value))
        return self
    
    def add_task(self, taskKey, key, value):
        self.task_values.append(TaskValue(taskKey=taskKey, key=key, value=value))
        return self
    
    
    def dbutils_config(self):
        a = ""
        a += "##### Setting DBUtils Widgets with Default value #####\n"
        for widget in self.widgets:
            a += f"dbutils.widgets.text('{widget.key}', '{widget.value}')\n"
        return a
    
    def create_tasks(self):
        notebooks = dict()
        for task in self.task_values:
            if task.taskKey not in notebooks:
                notebooks[task.taskKey] = create_notebook(task.taskKey)
            notebooks[task.taskKey]["cells"].append(create_cell(f"dbutils.jobs.taskValues.set(key = '{task.key}', value = '{task.value}')"))
        return notebooks