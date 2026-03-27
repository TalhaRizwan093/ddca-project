import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

def _resolve_current_dir() -> Path:
    # Databricks spark_python_task may execute without defining __file__.
    if "__file__" in globals():
        return Path(__file__).resolve().parent

    controller_path = os.getenv("DDCA_NIGHTLY_PIPELINE_PYTHON_FILE", "").strip().strip('"').strip("'")
    if controller_path.startswith("file:"):
        controller_path = controller_path[len("file:") :]

    if controller_path:
        return Path(controller_path).resolve().parent

    return (Path.cwd() / "code" / "orchestration").resolve()


CURRENT_DIR = _resolve_current_dir()
CODE_DIR = CURRENT_DIR.parent
PROJECT_ROOT = CODE_DIR.parent


def _arg_value(flag: str) -> str:
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1].strip()
    return ""

def _code_dir_candidates() -> list[Path]:
    candidates: list[Path] = []

    code_dir_arg = _arg_value("--code-dir")
    if code_dir_arg:
        candidates.append(Path(code_dir_arg))

    candidates.append(CODE_DIR)

    notebook_root = _clean_env(os.getenv("DATABRICKS_NOTEBOOK_ROOT"))
    if notebook_root:
        candidates.append(Path(notebook_root).parent)

    controller_file = _clean_env(os.getenv("DDCA_NIGHTLY_PIPELINE_PYTHON_FILE"))
    if controller_file.startswith("file:"):
        controller_file = controller_file[len("file:") :]
    if controller_file:
        controller_path = Path(controller_file)
        candidates.append(controller_path.parent.parent)

        parts = controller_path.parts
        if len(parts) >= 6 and parts[1:3] == ("Workspace", "Users"):
            user_name = parts[3]
            repo_name = parts[4]
            candidates.append(Path("/Workspace/Repos") / user_name / repo_name / "code")

    candidates.append((Path.cwd() / "code").resolve())

    # De-duplicate while preserving order.
    seen: set[str] = set()
    unique: list[Path] = []
    for path in candidates:
        key = str(path)
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def _prepare_python_path() -> list[tuple[str, bool]]:
    probe: list[tuple[str, bool]] = []
    for candidate in _code_dir_candidates():
        exists = candidate.exists()
        probe.append((str(candidate), exists))

        # Add all candidates so Databricks mount-path aliases can still resolve imports.
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))

    return probe


def _load_env_file(env_file: Path) -> None:
    for raw_line in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _load_env_from_candidates() -> list[str]:
    loaded: list[str] = []
    seen: set[str] = set()

    for code_dir in _code_dir_candidates():
        env_file = (code_dir.parent / ".env").resolve()
        env_file_key = str(env_file)
        if env_file_key in seen:
            continue
        seen.add(env_file_key)

        if env_file.is_file():
            _load_env_file(env_file)
            loaded.append(env_file_key)

    return loaded


def _is_truthy(value: str) -> bool:
    return value.lower() not in {"0", "false", "no", "off"}


def _validate_required_ingestion_env(run_id: str, loaded_env_files: list[str]) -> None:
    require_keys_raw = _clean_env(os.getenv("DDCA_REQUIRE_INGESTION_KEYS", "true"))
    if not _is_truthy(require_keys_raw):
        print(f"[{run_id}] Strict ingestion key validation disabled (DDCA_REQUIRE_INGESTION_KEYS={require_keys_raw}).")
        return

    required = ["GITHUB_TOKEN", "SO_API_KEY"]
    missing = [key for key in required if not _clean_env(os.getenv(key))]
    if missing:
        raise EnvironmentError(
            f"[{run_id}] Missing required ingestion environment variables: {missing}. "
            f"Loaded .env files: {loaded_env_files or 'none'}. "
            "Configure these on Databricks compute (cluster env vars or secret references)."
        )


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

    path_probe = _prepare_python_path()
    loaded_env_files = _load_env_from_candidates()
    _validate_required_ingestion_env(run_id, loaded_env_files)

    try:
        from ingestion.github_ingestor import run as run_github  # type: ignore
        from ingestion.pypi_ingestor import run as run_pypi  # type: ignore
        from ingestion.stackoverflow_ingestor import run as run_stackoverflow  # type: ignore
        from orchestration.databricks_job import DatabricksJobsClient  # type: ignore
        from upload_to_dbfs import upload_bronze  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Failed to import pipeline modules. "
            f"sys.path_probe={path_probe} "
            f"loaded_env_files={loaded_env_files} "
            f"cwd={Path.cwd()} "
            f"current_dir={CURRENT_DIR}"
        ) from exc

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
            f"Databricks job '{job_name}' not found. Create the worker job in Databricks Workflows first."
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
    main()
