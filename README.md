# kedro-pycafe-data

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

## Overview

A Kedro pipeline that extracts and processes **Kedro framework usage analytics** — pulling PyPI download statistics and Heap telemetry data from Snowflake, then outputting CSV files for dashboards and reporting.

The project tracks:
- PyPI download trends for the Kedro package (global and by country)
- New Kedro user adoption (monthly)
- Monthly active users segmented by Kedro version
- Plugin adoption rates (e.g. `kedro-mlflow`, `kedro-docker`, `kedro-airflow`, etc.)
- Core command usage patterns (`kedro run`, `kedro viz`, `kedro new`, etc.)

## Pipelines

### `data_transfer`

Extracts PyPI download data from Snowflake views and saves them locally as CSV:

| Node | Snowflake source | Output |
|---|---|---|
| `fetch_and_save_snowflake_data` | `KEDRO_BI_DB.PYPI.V_PYPI_KEDRO_DOWNLOADS` | `data/02_intermediate/pypi_kedro_downloads.csv` |
| `fetch_and_save_downloads_by_country` | `KEDRO_BI_DB.PYPI.V_DOWNLOADS_BY_COUNTRY` | `data/02_intermediate/downloads_by_country.csv` |
| `fetch_and_save_plugin_downloads` | `KEDRO_BI_DB.PYPI.V_PYPI_PLUGIN_DOWNLOADS` | `data/02_intermediate/pypi_plugin_downloads.csv` |

### `telemetry_data`

Processes Heap telemetry events from `HEAP_FRAMEWORK_VIZ_PRODUCTION.HEAP` in Snowflake and produces these outputs:

| Output | Description |
|---|---|
| `new_kedro_users_monthly.csv` | First-time Kedro users per month (filtered to users active >8 days) |
| `mau_kedro.csv` | Monthly active users segmented by Kedro version |
| `kedro_plugins_mau.csv` | Monthly unique users per plugin (11 plugins tracked) |
| `kedro_commands_mau.csv` | Monthly unique users per core command (7 commands tracked) |
| `cohort_retention.csv` | Monthly cohort retention matrix (long format): for each cohort month, how many qualified users were still active 0-12 months later, plus pre-computed `retention_pct`. Latest 2 cohorts suppressed while their `cohort_size` is still settling. |
| `experimental_dataset_usage_monthly.csv` | Monthly adoption of GenAI/experimental datasets (long format), one row per month × dataset class with `unique_users`, `project_runs` and `total_catalog_entries`. Sourced from the telemetry `dataset_type_count.*` properties (requires `kedro-telemetry` >= 0.8). Labelled with `namespace` (core/experimental), `is_genai` and a friendly `tool` group so a dashboard can filter the GenAI subset vs. all experimental datasets. Columns are picked by namespace prefix, so new datasets appear automatically. |
| `experimental_dataset_usage_summary.csv` | All-time per-dataset rollup of the above (distinct `unique_users`, `first_seen`/`last_seen`), plus pre-computed de-duplicated `ALL GenAI datasets` and `ALL experimental datasets` total rows. Per-dataset rows below `genai_min_users` distinct users are suppressed (k-anonymity). |
| `experimental_tool_usage_summary.csv` | All-time per-**tool** rollup (e.g. Langfuse's Prompt/Trace/Evaluation collapsed into one `Langfuse` row) with **de-duplicated** `unique_users`/`project_runs` and `first_seen`/`last_seen`. Powers the dashboard's "distinct users by tool" chart. |

## Prerequisites

- Python 3.9+
- Access to the Snowflake data warehouse with appropriate credentials
- Environment variables for Snowflake authentication (account, user, password)

## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

Configure Snowflake credentials in `conf/local/credentials.yml` (not committed to version control).

## Usage

Run all pipelines:

```bash
kedro run
```

Run a specific pipeline:

```bash
kedro run --pipeline data_transfer
kedro run --pipeline telemetry_data
```

## Automated daily export (GitHub Actions)

A [scheduled workflow](.github/workflows/snowflake_queries.yml) runs every day at **07:15 UTC** to keep the CSV files up to date automatically.

**What it does:**

1. Checks out the repo on an `ubuntu-latest` runner.
2. Installs Python 3.11 and project dependencies (via `uv`).
3. Runs `kedro run --pipeline data_transfer` to refresh PyPI download CSVs.
4. Runs `kedro run --pipeline telemetry_data` to refresh telemetry CSVs.
5. Commits and pushes any updated CSVs back to `main` (commit message: `Update pipeline outputs [skip ci]`).

**Required GitHub secrets:**

| Secret | Description |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_ROLE` | Snowflake role |

The workflow can also be triggered manually via `workflow_dispatch`.

## Project layout

```
.github/workflows/      # GitHub Actions (daily Snowflake export)
conf/base/              # Catalog, parameters, and credential templates
data/02_intermediate/   # Output CSV files
src/kedro_pycafe_data/
  pipelines/
    data_transfer/      # Snowflake → CSV for PyPI download stats
    telemetry_data/     # Snowflake Heap telemetry → usage analytics CSVs
```
