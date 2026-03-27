import os
import time
from typing import Any

import requests


TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def _clean_env(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().strip('"').strip("'")


class DatabricksJobsClient:
    def __init__(self) -> None:
        self.host = _clean_env(os.getenv("DATABRICKS_HOST"))
        self.token = _clean_env(os.getenv("DATABRICKS_TOKEN"))

        if not self.host or not self.token:
            raise EnvironmentError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

        self.base_url = self.host.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        response = requests.request(
            method=method,
            url=url,
            headers=self.headers,
            params=params,
            json=payload,
            timeout=120,
        )

        if not response.ok:
            raise RuntimeError(f"Databricks API failed ({response.status_code}) {path}: {response.text}")

        if not response.text:
            return {}

        return response.json()

    def list_jobs(self) -> list[dict[str, Any]]:
        data = self._request("GET", "/api/2.1/jobs/list")
        return data.get("jobs", [])

    def get_job_id_by_name(self, job_name: str) -> int | None:
        for job in self.list_jobs():
            settings = job.get("settings", {})
            if settings.get("name") == job_name:
                return job.get("job_id")
        return None

    def create_job(self, settings: dict[str, Any]) -> int:
        data = self._request("POST", "/api/2.1/jobs/create", payload=settings)
        return int(data["job_id"])

    def reset_job(self, job_id: int, settings: dict[str, Any]) -> None:
        payload = {
            "job_id": job_id,
            "new_settings": settings,
        }
        self._request("POST", "/api/2.1/jobs/reset", payload=payload)

    def run_now(self, job_id: int) -> int:
        payload = {"job_id": job_id}
        data = self._request("POST", "/api/2.1/jobs/run-now", payload=payload)
        return int(data["run_id"])

    def get_run(self, run_id: int) -> dict[str, Any]:
        return self._request("GET", "/api/2.1/jobs/runs/get", params={"run_id": run_id})

    def wait_for_run(self, run_id: int, poll_seconds: int = 30, timeout_seconds: int = 7200) -> dict[str, Any]:
        started = time.time()

        while True:
            run_data = self.get_run(run_id)
            state = run_data.get("state", {})
            life_cycle_state = state.get("life_cycle_state", "")
            result_state = state.get("result_state", "")
            state_message = state.get("state_message", "")

            print(
                f"Run {run_id} status: life_cycle_state={life_cycle_state} "
                f"result_state={result_state} message={state_message}"
            )

            if life_cycle_state in TERMINAL_STATES:
                return run_data

            if time.time() - started > timeout_seconds:
                raise TimeoutError(f"Run {run_id} did not finish within {timeout_seconds} seconds")

            time.sleep(poll_seconds)
