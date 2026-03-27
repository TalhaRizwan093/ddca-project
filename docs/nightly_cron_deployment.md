# Nightly Cron Deployment (Append Same Files + Full Medallion Refresh)

## Goal

Run a nightly pipeline that does this in order:

1. Run ingestion scripts (GitHub, PyPI, Stack Overflow).
2. Append new snapshots into the same local JSON files (per package/source).
3. Upload those same files to DBFS FileStore with overwrite enabled.
4. Run Databricks Bronze, Silver, and Gold notebooks in sequence/dependencies.

This keeps static file names while retaining history inside each JSON file.

## What Was Implemented

- Ingestors now append records to stable files instead of replacing with single object.
- Upload utility still overwrites DBFS file paths (same filenames) each night.
- Databricks job deployment script creates/updates a medallion refresh job.
- Nightly orchestrator script runs ingestion, upload, and Databricks job run-now.
- Cron scripts are provided for Linux installation.

## New/Updated Files

- `code/ingestion/github_ingestor.py`
- `code/ingestion/pypi_ingestor.py`
- `code/ingestion/stackoverflow_ingestor.py`
- `code/upload_to_dbfs.py`
- `code/orchestration/databricks_job.py`
- `code/orchestration/deploy_databricks_job.py`
- `code/orchestration/nightly_pipeline.py`
- `ops/nightly_pipeline.sh`
- `ops/install_cron.sh`

## One-Time Setup

1. Fill `.env` with valid values:
   - `GITHUB_TOKEN`
   - `SO_API_KEY`
   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`
   - `DATABRICKS_CLUSTER_ID`
   - `DATABRICKS_NOTEBOOK_ROOT`

2. Deploy or update the Databricks medallion job:

```bash
python code/orchestration/deploy_databricks_job.py
```

This job contains tasks:

- Bronze: `01_bronze_to_delta.ipynb`
- Silver: `02_silver_github.ipynb`, `03_silver_pypi.ipynb`, `04_silver_stackoverflow.ipynb`
- Gold: `05_gold_health_score.ipynb`

3. Run one manual end-to-end test:

```bash
python code/orchestration/nightly_pipeline.py
```

## Configure API Keys On Databricks Compute (Manual)

When controller code runs on Databricks cluster compute, local machine `.env` is not automatically available.

1. Create a Databricks secret scope (once):

```bash
databricks secrets create-scope ddca
```

2. Add secrets:

```bash
databricks secrets put-secret ddca github_token
databricks secrets put-secret ddca so_api_key
```

3. In Databricks UI, open your cluster and set environment variables:

- `GITHUB_TOKEN={{secrets/ddca/github_token}}`
- `SO_API_KEY={{secrets/ddca/so_api_key}}`

4. Restart the cluster.

5. Keep strict validation enabled in `.env` (recommended):

- `DDCA_REQUIRE_INGESTION_KEYS="true"`

With strict validation enabled, controller runs fail fast if `GITHUB_TOKEN` or `SO_API_KEY` are missing at runtime.

## Deploy Scheduled Cron-Like Run On Databricks

Use Databricks Workflows schedule (Quartz cron) instead of OS cron when you want scheduling inside Databricks.

1. Open Databricks Workspace and make sure this repo is available in Repos.
2. Ensure your Databricks job exists by running:

```bash
python code/orchestration/deploy_databricks_job.py
```

3. Deploy the scheduled Databricks controller job:

```bash
python code/orchestration/deploy_databricks_cron_job.py
```

4. Default Quartz cron expression is daily at 02:00 UTC:

```text
0 0 2 * * ?
```

5. You can customize the schedule/timezone by setting these env vars before deployment:

- `DDCA_CRON_QUARTZ_EXPR`
- `DDCA_CRON_TIMEZONE`
- `DDCA_CRON_PAUSE_STATUS`

6. In Databricks UI, open Workflows and verify two jobs exist:

- Medallion worker job: `ddca-medallion-refresh`
- Scheduled controller job: `ddca-nightly-controller`

7. Click Run now on the controller job once to validate the full chain.

Optional API-first schedule update:

- If you prefer code-only deployment, keep using `deploy_databricks_cron_job.py` and redeploy after changing schedule env vars.

## Enable Nightly Cron (Linux)

1. Make scripts executable:

```bash
chmod +x ops/nightly_pipeline.sh ops/install_cron.sh
```

2. Install cron entry (default schedule is 02:00 daily):

```bash
./ops/install_cron.sh
```

3. Optional custom schedule:

```bash
DDCA_CRON_SCHEDULE="0 1 * * *" ./ops/install_cron.sh
```

## How It Works Nightly

- Appends snapshots into same local file names such as:
  - `data/bronze/github/contributors/django.json`
  - `data/bronze/pypi/metadata/django.json`
  - `data/bronze/stackoverflow/questions/django.json`
- Uploads those files to `dbfs:/FileStore/bronze/...` as overwrite.
- Bronze reads full file content and overwrites Bronze tables.
- Silver and Gold run after Bronze and overwrite downstream tables.

## Logs and Observability

- Orchestrator summary: `data/logs/nightly_pipeline_summary.jsonl`
- Cron execution log: `data/logs/nightly_cron.log`
- Source structured logs:
  - `data/logs/github_ingestor_structured.jsonl`
  - `data/logs/pypi_ingestor_structured.jsonl`
  - `data/logs/stackoverflow_ingestor_structured.jsonl`

## Notes

- This design intentionally increases processing volume over time because each stable file grows daily.
- It keeps your Bronze file naming simple and unchanged.
- You can optimize later by archiving old snapshots or moving to partitioned incremental ingestion.
