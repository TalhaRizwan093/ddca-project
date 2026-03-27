#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${DDCA_PROJECT_DIR:-/opt/ddca2026-project-online5}"
PYTHON_BIN="${DDCA_PYTHON_BIN:-${PROJECT_DIR}/.venv/bin/python}"
LOG_FILE="${DDCA_CRON_LOG_FILE:-${PROJECT_DIR}/data/logs/nightly_cron.log}"

mkdir -p "$(dirname "${LOG_FILE}")"
cd "${PROJECT_DIR}"

"${PYTHON_BIN}" code/orchestration/nightly_pipeline.py >> "${LOG_FILE}" 2>&1
