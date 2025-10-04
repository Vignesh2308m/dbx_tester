from databricks.sdk import WorkspaceClient

def get_workspace_client():
    w = WorkspaceClient()
    return w