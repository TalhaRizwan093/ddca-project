#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${DDCA_PROJECT_DIR:-/opt/ddca2026-project-online5}"
SCHEDULE="${DDCA_CRON_SCHEDULE:-0 2 * * *}"
CRON_ENTRY="${SCHEDULE} DDCA_PROJECT_DIR=${PROJECT_DIR} ${PROJECT_DIR}/ops/nightly_pipeline.sh"

( crontab -l 2>/dev/null | grep -v "nightly_pipeline.sh"; echo "${CRON_ENTRY}" ) | crontab -

echo "Installed cron entry:"
crontab -l | grep "nightly_pipeline.sh"
