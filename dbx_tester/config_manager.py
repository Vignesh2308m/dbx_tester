from dbx_tester.utils.databricks_api import notebook_builder
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class Widget:
    """Represents a widget with a key and value.
    
    Attributes:
        key: The key of the widget.
        value: The value of the widget.
    """
    key: str
    value: str


@dataclass
class TaskValue:
    """Represents a task value with a task key, key, and value.
    
    Attributes:
        task_key: The task key associated with the value.
        key: The key of the task value.
        value: The value of the task.
    """
    task_key: str
    key: str
    value: str


@dataclass
class RunParameter:
    """Represents a run parameter with a key and value.
    
    Attributes:
        key: The key of the run parameter.
        value: The value of the run parameter.
    """
    key: str
    value: Any


class NotebookConfigManager:
    """Manages notebook configuration including widgets and task values."""
    
    def __init__(self) -> None:
        """Initializes the NotebookConfigManager with empty widgets and task values."""
        self._widgets: List[Widget] = []
        self._task_values: List[TaskValue] = []
    
    @property
    def widgets(self) -> List[Widget]:
        """Returns a copy of the current widgets."""
        return self._widgets.copy()
    
    @property
    def task_values(self) -> List[TaskValue]:
        """Returns a copy of the current task values."""
        return self._task_values.copy()
    
    def add_widget(self, key: str, value: str) -> 'NotebookConfigManager':
        """Adds a widget to the configuration.
        
        Args:
            key: The key of the widget.
            value: The value of the widget.
            
        Returns:
            The instance of the manager for method chaining.
        """
        widget = Widget(key=str(key), value=str(value))
        self._widgets.append(widget)
        return self
    
    def add_widgets(self, widgets: List[tuple]) -> 'NotebookConfigManager':
        """Adds multiple widgets to the configuration.
        
        Args:
            widgets: List of tuples containing (key, value) pairs.
            
        Returns:
            The instance of the manager for method chaining.
        """
        for key, value in widgets:
            self.add_widget(key, value)
        return self
    
    def add_task_value(self, task_key: str, key: str, value: str) -> 'NotebookConfigManager':
        """Adds a task value to the configuration.
        
        Args:
            task_key: The task key.
            key: The key of the task value.
            value: The value of the task.
            
        Returns:
            The instance of the manager for method chaining.
        """
        task_value = TaskValue(task_key=task_key, key=key, value=value)
        self._task_values.append(task_value)
        return self
    
    def clear_widgets(self) -> 'NotebookConfigManager':
        """Clears all widgets from the configuration.
        
        Returns:
            The instance of the manager for method chaining.
        """
        self._widgets.clear()
        return self
    
    def clear_task_values(self) -> 'NotebookConfigManager':
        """Clears all task values from the configuration.
        
        Returns:
            The instance of the manager for method chaining.
        """
        self._task_values.clear()
        return self
    
    def generate_dbutils_config(self) -> str:
        """Generates the DBUtils widget configuration script.
        
        Returns:
            The script for setting DBUtils widgets.
        """
        if not self._widgets:
            return "##### No DBUtils Widgets configured #####\n"
        
        lines = ["##### Setting DBUtils Widgets with Default value #####"]
        for widget in self._widgets:
            lines.append(f"dbutils.widgets.text('{widget.key}', '{widget.value}')")
        
        return "\n".join(lines) + "\n"
    
    def create_task_notebooks(self) -> Dict[str, Any]:
        """Creates tasks and returns a dictionary of notebooks.
        
        Returns:
            A dictionary with task keys and their corresponding notebooks.
        """
        notebooks = {}
        
        for task_value in self._task_values:
            task_key = task_value.task_key
            
            if task_key not in notebooks:
                notebooks[task_key] = notebook_builder(task_key)
            
            cell_content = (
                f"dbutils.jobs.taskValues.set("
                f"key='{task_value.key}', "
                f"value='{task_value.value}')"
            )
            notebooks[task_key].add_cell(cell_content)
        
        return notebooks


class JobConfigManager:
    """Manages job configuration including run parameters."""
    
    def __init__(self) -> None:
        """Initializes the JobConfigManager with empty run parameters."""
        self._run_parameters: List[RunParameter] = []
    
    @property
    def run_parameters(self) -> List[RunParameter]:
        """Returns a copy of the current run parameters."""
        return self._run_parameters.copy()
    
    def add_parameter(self, key: str, value: Any) -> 'JobConfigManager':
        """Adds a run parameter to the configuration.
        
        Args:
            key: The key of the run parameter.
            value: The value of the run parameter.
            
        Returns:
            The instance of the manager for method chaining.
        """
        parameter = RunParameter(key=key, value=value)
        self._run_parameters.append(parameter)
        return self
    
    def add_parameters(self, parameters: Dict[str, Any]) -> 'JobConfigManager':
        """Adds multiple run parameters to the configuration.
        
        Args:
            parameters: Dictionary of key-value pairs to add as parameters.
            
        Returns:
            The instance of the manager for method chaining.
        """
        for key, value in parameters.items():
            self.add_parameter(key, value)
        return self
    
    def remove_parameter(self, key: str) -> 'JobConfigManager':
        """Removes a run parameter by key.
        
        Args:
            key: The key of the parameter to remove.
            
        Returns:
            The instance of the manager for method chaining.
        """
        self._run_parameters = [
            param for param in self._run_parameters 
            if param.key != key
        ]
        return self
    
    def clear_parameters(self) -> 'JobConfigManager':
        """Clears all run parameters from the configuration.
        
        Returns:
            The instance of the manager for method chaining.
        """
        self._run_parameters.clear()
        return self
    
    def get_job_config(self) -> Dict[str, Any]:
        """Generates the job configuration dictionary.
        
        Returns:
            A dictionary of run parameters.
        """
        return {param.key: param.value for param in self._run_parameters}
    
    def has_parameter(self, key: str) -> bool:
        """Checks if a parameter with the given key exists.
        
        Args:
            key: The key to check for.
            
        Returns:
            True if the parameter exists, False otherwise.
        """
        return any(param.key == key for param in self._run_parameters)