import os
from pathlib import Path

from databricks_job import DatabricksJobsClient


def _clean_env(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().strip('"').strip("'")


def _notebook_task(task_key: str, notebook_path: str, cluster_id: str, depends_on: list[str] | None = None) -> dict:
    task = {
        "task_key": task_key,
        "existing_cluster_id": cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path,
        },
        "timeout_seconds": 0,
        "max_retries": 0,
        "retry_on_timeout": False,
    }

    if depends_on:
        task["depends_on"] = [{"task_key": key} for key in depends_on]

    return task


def build_job_settings() -> dict:
    project_root = Path(__file__).resolve().parents[2]
    default_notebook_root = "/Workspace/Repos/<your-user>/ddca2026-project-online5/code/databricks"

    cluster_id = _clean_env(os.getenv("DATABRICKS_CLUSTER_ID"))
    notebook_root = _clean_env(os.getenv("DATABRICKS_NOTEBOOK_ROOT", default_notebook_root)).rstrip("/")
    job_name = _clean_env(os.getenv("DDCA_DATABRICKS_JOB_NAME", "ddca-medallion-refresh"))

    if not cluster_id:
        raise EnvironmentError("DATABRICKS_CLUSTER_ID must be set")

    tasks = [
        _notebook_task("bronze_refresh", f"{notebook_root}/01_bronze_to_delta.ipynb", cluster_id),
        _notebook_task("silver_github", f"{notebook_root}/02_silver_github.ipynb", cluster_id, ["bronze_refresh"]),
        _notebook_task("silver_pypi", f"{notebook_root}/03_silver_pypi.ipynb", cluster_id, ["bronze_refresh"]),
        _notebook_task("silver_stackoverflow", f"{notebook_root}/04_silver_stackoverflow.ipynb", cluster_id, ["bronze_refresh"]),
        _notebook_task("gold_health_score", f"{notebook_root}/05_gold_health_score.ipynb", cluster_id, ["silver_github", "silver_pypi", "silver_stackoverflow"]),
    ]

    return {
        "name": job_name,
        "max_concurrent_runs": 1,
        "timeout_seconds": 0,
        "tasks": tasks,
        "tags": {
            "project": "ddca2026",
            "pipeline": "medallion_refresh",
            "deployment_source": str(project_root),
        },
    }


def main() -> int:
    client = DatabricksJobsClient()
    settings = build_job_settings()

    job_name = settings["name"]
    existing_job_id = client.get_job_id_by_name(job_name)

    if existing_job_id is None:
        job_id = client.create_job(settings)
        print(f"Created job '{job_name}' with job_id={job_id}")
    else:
        client.reset_job(existing_job_id, settings)
        job_id = existing_job_id
        print(f"Updated job '{job_name}' with job_id={job_id}")

    print("Deployment successful")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
