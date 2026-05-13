# Plugin downloads — cross-repo artifacts

Adds a yearly PyPI-downloads view for Kedro plugins to the dashboard, mirroring
the existing per-Kedro `pypi_kedro_downloads` flow.

This folder collects pieces that need to land in **other** places so they
don't get lost. The Kedro-side wiring in this repo (catalog, pipeline node,
workflow, README) is on the same branch as this file.

## Rollout order

1. **Snowflake — create the view.** Run [`upstream_view.sql`](./upstream_view.sql)
   directly in Snowflake (web UI or SnowSQL), as a user with `CREATE VIEW`
   on `KEDRO_BI_DB.PYPI`. There is **no code change** in
   `McK-Internal/kedro-pypi-to-snowflake` — its `AGGREGATED_DOWNLOADS` table
   already loads all 9 plugins (see that repo's
   `conf/base/catalog.yml` → `bigquery_pypi_downloads.sql`), and the view
   just filters and rolls that table up.

   Sanity-check immediately after creating the view:

   ```sql
   SELECT project, COUNT(*) AS rows
   FROM   KEDRO_BI_DB.PYPI.V_PYPI_PLUGIN_DOWNLOADS
   GROUP  BY project;
   ```

   Expect 9 rows, one per plugin, each non-zero.

2. **This repo** — merge the `feature/plugin-downloads` PR. The next cron
   tick (or a manual `workflow_dispatch`) will produce
   `data/02_intermediate/pypi_plugin_downloads.csv` and commit it to `main`.

3. **PyCafe** — open `py.cafe/kedro-lfai/kedro-stats` and paste the page
   snippet from [`pycafe_page.py`](./pycafe_page.py) into `app.py`, wiring
   it into the dashboard's `pages=[...]` list.

## Plugin list (9)

`kedro-mlflow`, `kedro-docker`, `kedro-airflow`, `kedro-databricks`,
`kedro-azureml`, `kedro-vertexai`, `kedro-boot`, `kedro-sagemaker`,
`kedro-kubeflow`.

`kedro-coda` and `kedro-gql` are intentionally excluded for now.
