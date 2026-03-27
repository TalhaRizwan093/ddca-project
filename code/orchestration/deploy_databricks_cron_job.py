import os

from databricks_job import DatabricksJobsClient


def _clean_env(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().strip('"').strip("'")


def build_cron_job_settings() -> dict:
    cluster_id = _clean_env(os.getenv("DATABRICKS_CLUSTER_ID"))
    if not cluster_id:
        raise EnvironmentError("DATABRICKS_CLUSTER_ID must be set")

    controller_job_name = _clean_env(os.getenv("DDCA_DATABRICKS_CRON_JOB_NAME", "ddca-nightly-controller"))
    controller_python_file = _clean_env(
        os.getenv(
            "DDCA_NIGHTLY_PIPELINE_PYTHON_FILE",
            "file:/Workspace/Repos/<your-user>/ddca2026-project-online5/code/orchestration/nightly_pipeline.py",
        )
    )
    cron_expr = _clean_env(os.getenv("DDCA_CRON_QUARTZ_EXPR", "0 0 2 * * ?"))
    timezone_id = _clean_env(os.getenv("DDCA_CRON_TIMEZONE", "UTC"))
    pause_status = _clean_env(os.getenv("DDCA_CRON_PAUSE_STATUS", "UNPAUSED"))

    return {
        "name": controller_job_name,
        "max_concurrent_runs": 1,
        "timeout_seconds": 0,
        "tasks": [
            {
                "task_key": "nightly_controller",
                "existing_cluster_id": cluster_id,
                "spark_python_task": {
                    "python_file": controller_python_file,
                },
                "timeout_seconds": 0,
                "max_retries": 0,
                "retry_on_timeout": False,
            }
        ],
        "schedule": {
            "quartz_cron_expression": cron_expr,
            "timezone_id": timezone_id,
            "pause_status": pause_status,
        },
        "tags": {
            "project": "ddca2026",
            "pipeline": "nightly_controller",
        },
    }


def main() -> int:
    client = DatabricksJobsClient()
    settings = build_cron_job_settings()

    job_name = settings["name"]
    existing_job_id = client.get_job_id_by_name(job_name)

    if existing_job_id is None:
        job_id = client.create_job(settings)
        print(f"Created scheduled controller job '{job_name}' with job_id={job_id}")
    else:
        client.reset_job(existing_job_id, settings)
        job_id = existing_job_id
        print(f"Updated scheduled controller job '{job_name}' with job_id={job_id}")

    print("Scheduled deployment successful")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
