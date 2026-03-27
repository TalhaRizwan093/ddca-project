import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
CODE_DIR = CURRENT_DIR.parent
PROJECT_ROOT = CODE_DIR.parent

if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from ingestion.github_ingestor import run as run_github  # noqa: E402
from ingestion.pypi_ingestor import run as run_pypi  # noqa: E402
from ingestion.stackoverflow_ingestor import run as run_stackoverflow  # noqa: E402
from orchestration.databricks_job import DatabricksJobsClient  # noqa: E402
from upload_to_dbfs import upload_bronze  # noqa: E402


def _clean_env(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().strip('"').strip("'")


def _run_ingestion_source(source_name: str, fn):
    started = datetime.now(timezone.utc)
    try:
        fn()
        return {
            "source": source_name,
            "status": "success",
            "started_at": started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error": None,
        }
    except Exception as exc:
        return {
            "source": source_name,
            "status": "error",
            "started_at": started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }


def _write_summary(summary: dict) -> None:
    logs_path = PROJECT_ROOT / "data" / "logs"
    logs_path.mkdir(parents=True, exist_ok=True)
    summary_file = logs_path / "nightly_pipeline_summary.jsonl"
    with open(summary_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(summary, ensure_ascii=False) + "\n")


def main() -> int:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now(timezone.utc)

    print(f"[{run_id}] Starting nightly pipeline")

    ingestion_results = [
        _run_ingestion_source("github", run_github),
        _run_ingestion_source("pypi", run_pypi),
        _run_ingestion_source("stackoverflow", run_stackoverflow),
    ]

    ingestion_errors = [item for item in ingestion_results if item["status"] == "error"]
    if ingestion_errors:
        print(f"[{run_id}] Ingestion completed with {len(ingestion_errors)} source errors (best-effort mode continues)")

    upload_summary = upload_bronze()
    print(f"[{run_id}] Upload complete: uploaded={upload_summary['uploaded']} errors={upload_summary['errors']}")

    if upload_summary["errors"] > 0:
        raise RuntimeError("Upload to DBFS failed for one or more files")

    job_name = _clean_env(os.getenv("DDCA_DATABRICKS_JOB_NAME", "ddca-medallion-refresh"))
    poll_seconds = int(_clean_env(os.getenv("DDCA_POLL_SECONDS", "30")))
    timeout_seconds = int(_clean_env(os.getenv("DDCA_RUN_TIMEOUT_SECONDS", "7200")))

    client = DatabricksJobsClient()
    job_id = client.get_job_id_by_name(job_name)
    if job_id is None:
        raise RuntimeError(
            f"Databricks job '{job_name}' not found. Run code/orchestration/deploy_databricks_job.py first."
        )

    run_now_id = client.run_now(job_id)
    print(f"[{run_id}] Triggered Databricks job run_id={run_now_id}")

    run_data = client.wait_for_run(run_now_id, poll_seconds=poll_seconds, timeout_seconds=timeout_seconds)
    state = run_data.get("state", {})
    result_state = state.get("result_state", "UNKNOWN")

    summary = {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "ingestion_results": ingestion_results,
        "upload_summary": upload_summary,
        "databricks_job_name": job_name,
        "databricks_job_id": job_id,
        "databricks_run_id": run_now_id,
        "databricks_result_state": result_state,
    }
    _write_summary(summary)

    if result_state != "SUCCESS":
        raise RuntimeError(f"Databricks run failed with result_state={result_state}")

    print(f"[{run_id}] Nightly pipeline completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
