from pathlib import Path
from databricks.sdk.service import jobs

from dbx_tester.utils.api import get_workspace_client


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

    
class JobRunner():
    def __init__(self, job_id, params = {}):
        self.job_id = job_id
        self.run_id = None

    def run(self):
        w = get_workspace_client()
        run_id = w.jobs.run_now(job_id=self.job_id, params = {}).run_id
        return run_id
    
    def get_run_status(self):
        w = get_workspace_client()
        run = w.jobs.get_run(run_id=self.run_id)
        return run.state.life_cycle_state, run.state.result_state