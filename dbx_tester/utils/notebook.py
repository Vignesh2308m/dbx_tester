from pathlib import Path
from databricks.sdk.service import workspace, jobs
from databricks.sdk.service.workspace import ObjectType
import base64
import json
import uuid


from dbx_tester.utils.api import get_workspace_client

class notebook_builder:
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