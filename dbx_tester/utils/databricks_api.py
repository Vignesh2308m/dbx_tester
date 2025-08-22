from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace, jobs
from databricks.sdk.service.workspace import ObjectType

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import base64
import json
import uuid

def get_workspace_client():
    w = WorkspaceClient()
    return w

def decode_notebook(path):

    w = get_workspace_client()

    notebook_content = w.workspace.export(
        path= path,
        format=workspace.ExportFormat.JUPYTER
    )

    decoded_bytes = base64.b64decode(notebook_content.content)

    decoded_string = decoded_bytes.decode('utf-8')

    notebook_dict = json.loads(decoded_string)
    
    return notebook_dict



class create_notebook:
    def __init__(self, name:str):
        self.workspace_client = get_workspace_client()

        self._notebook_dict = {
            "cells": [],
            "metadata": {
                "application/vnd.databricks.v1+notebook": {
                    "computePreferences": "null",
                    "dashboards": [],
                    "environmentMetadata": {
                        "base_environment": "",
                        "environment_version": "2"
                    },
                    "inputWidgetPreferences": "null",
                    "language": "python",
                    "notebookMetadata": {
                        "pythonIndentUnit": 4
                    },
                    "notebookName": name,
                    "widgets": {}
                },
                "language_info": {
                    "name": "python"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 0
        }
    def add_cell(self, cell):
        self._notebook_dict['cells'].append(create_cell(cell))

    def save_notebook(self, path):

        out_str = json.dumps(self._notebook_dict)

        out_utf8 = out_str.encode('utf-8')

        encoded_bytes = base64.b64encode(out_utf8).decode('utf-8')

        self.workspace_client.workspace.import_(
            path=path
            , content=encoded_bytes,
            overwrite=True,
            format=workspace.ExportFormat.JUPYTER
        )

    

def create_cell(code:str):
    return {
            "cell_type": "code",
            "execution_count": 0,
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "cellMetadata": {},
                    "inputWidgets": {},
                    "nuid": str(uuid.uuid4()),
                    "showTitle": "false",
                    "tableResultSettingsMap": {},
                    "title": ""
                }
            },
            "outputs": [],
            "source": [code]
        }
    
def get_notebook_path():
    dbutils = DBUtils(SparkSession.builder.getOrCreate())
    return "/Workspace"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

def is_notebook(path):
    w = get_workspace_client()
    return w.workspace.get_status(path=path).object_type == ObjectType.NOTEBOOK

def run_notebook(path, params={}):
    dbutils = DBUtils(SparkSession.builder.getOrCreate())
    dbutils.notebook.run(path=path, timeout_seconds=0, arguments=params)

class submit_run:
    def __init__(self, name, cluster_id):
        self.name = name
        self.tasks = []
        self.cluster_id = cluster_id
        self.workspace_client = get_workspace_client()
    
    def add_task(self, task_key, notebook_path):
        self.tasks.append(
            jobs.SubmitTask(
                existing_cluster_id=self.cluster_id,
                notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
                task_key=task_key,
            )
        )

    def run(self):
        return self.workspace_client.jobs.submit(
            run_name = self.name,
            tasks = self.tasks
        ).result()