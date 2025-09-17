from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace, jobs
from databricks.sdk.service.workspace import ObjectType

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pathlib import Path
import base64
import json
import uuid


def get_workspace_client():
    w = WorkspaceClient()
    return w

class NotebookBuilder:
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
        self._notebook_dict['cells'].append(self.create_cell(cell))

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

    def create_cell(self, code:str):
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
    try:
        w = get_workspace_client()
        if path.endswith(".ipynb"):
            path = path.split(".")[0]
        return w.workspace.get_status(path=path).object_type == ObjectType.NOTEBOOK
    except:
        return False
    
def get_job_id(name):
    w = get_workspace_client()
    for job in w.jobs.list():
        if job.settings.name == name:
            return job.job_id
    else:
        raise ValueError(f"JOB NOT FOUND: Job name {name} not found")

def is_job(name = None, job_id = None):
    w = get_workspace_client()
    if job_id is not None:
        try:
            w.jobs.get(job_id=job_id)
            return True
        except:
            return False
    elif name is not None:
        for job in w.jobs.list():
            if job.settings.name == name:
                return True
        else:
            return False
    else:
        raise ValueError("Either job name or job id must be provided")
    
def run_notebook(path, params={}):
    dbutils = DBUtils(SparkSession.builder.getOrCreate())
    dbutils.notebook.run(path=path, timeout_seconds=0, arguments=params)

def validate_cluster(cluster_name):
    w = get_workspace_client()
    if cluster_name is None:
        return None
    for cluster in w.clusters.list():
        if cluster.cluster_name == cluster_name:
            return cluster.cluster_id
    else:
        raise ValueError(f"CLUSTER NOT FOUND: Cluster name {cluster_name} not found")

class submit_run:
    def __init__(self, name, cluster_id = None):
        self.name = name
        self.tasks = []
        self.cluster_id = cluster_id
        self.workspace_client = get_workspace_client()
    
    def add_task(self, task_key, notebook_path:Path, params = {}, depend_on = None, cluster_id = None):
        self.tasks.append(
            jobs.SubmitTask(
                existing_cluster_id=cluster_id if cluster_id else self.cluster_id,
                notebook_task=jobs.NotebookTask(notebook_path=notebook_path, base_parameters=params),
                task_key=task_key,
                depends_on=[jobs.TaskDependency(task_key=i) for i in depend_on] if depend_on is not None else None
            )
        )

    def run(self):
        return self.workspace_client.jobs.submit(
            run_name = self.name,
            tasks = self.tasks
        )
    
    def as_dict(self):
        return {
            "run_name": self.name,
            "cluster_id": self.cluster_id,
            "tasks": [task.as_dict() for task in self.tasks]
        }