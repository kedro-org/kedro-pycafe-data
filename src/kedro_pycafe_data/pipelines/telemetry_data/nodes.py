import os
from typing import Tuple
from snowflake.snowpark import Session
import pandas as pd

# Earliest cohort month included in the cohort retention analysis. Anything
# before this date is excluded so the heatmap focuses on cohorts with full
# Heap telemetry coverage.
COHORT_START_MONTH = "2024-11"

# How many of the most-recent monthly cohorts to suppress from the cohort
# retention output. Recent cohorts have an unstable `cohort_size` because
# late-qualifiers (users who eventually exceed the >8-day activity span) keep
# joining the cohort retroactively. Two months gives those denominators time
# to settle.
COHORT_TRAILING_HIDE_MONTHS = 2


def build_telemetry_data() -> Tuple[
    pd.DataFrame,  # new_users_df            -> new_kedro_users_monthly.csv
    pd.DataFrame,  # mau_df                  -> mau_kedro.csv
    pd.DataFrame,  # plugins_mau_df          -> kedro_plugins_mau.csv
    pd.DataFrame,  # commands_mau_df         -> kedro_commands_mau.csv
    pd.DataFrame,  # cohort_retention_df     -> cohort_retention.csv
]:
    """Run the Snowflake telemetry queries and return five dataframes.

    Pipeline outline:
      Step 1-5  Build qualified-user temp tables (>8 days of non-CI activity
                on a real release version of Kedro).
      Step 6    Cohort retention matrix in long format — one row per
                (cohort_month, month_offset) including explicit zero-retention
                cells; latest two cohorts suppressed for stability.
      Final     New users, MAU, plugin MAU and core-command MAU dataframes.
    """

    # Add hardcoded DB and schema
    connection_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": "HEAP_NTD_KEDRO",
        "warehouse": "HEAP_NTD_KEDRO_WH",
        "database": "DEMO_DB",
        "schema": "PUBLIC",
    }

    session = Session.builder.configs(connection_params).create()

    # --- Step 1 ---
    # The RLIKE predicate below keeps only rows whose PROJECT_VERSION looks
    # like a real release number (starts with `<digits>.<digit>`, e.g. 0.19,
    # 1.2.6, 1.10.0). It drops placeholder strings like "test", "dev", "main"
    
    session.sql("""
        CREATE OR REPLACE TEMPORARY TABLE temp_dt_username AS
        SELECT DATE(time) AS dt,
               username,
               MAX(LEFT(PROJECT_VERSION, 4)) AS max_version_prefix,
               COUNT(*) AS cnt
        FROM HEAP_FRAMEWORK_VIZ_PRODUCTION.HEAP.KEDRO_PROJECT_STATISTICS
        WHERE DATE(time) >= '2024-09-01'
          AND (is_ci_env IS NULL OR is_ci_env = 'false')
          AND PROJECT_VERSION RLIKE '^[0-9]+[.][0-9].*$'
        GROUP BY 1, 2
    """).collect()

    # --- Step 2 ---
    session.sql("""
        CREATE OR REPLACE TEMPORARY TABLE temp_username_min_max_dt AS
        SELECT username,
               MIN(dt) AS min_dt,
               MAX(dt) AS max_dt
        FROM temp_dt_username
        GROUP BY 1
    """).collect()

    # --- Step 3 ---
    session.sql("""
        CREATE OR REPLACE TEMPORARY TABLE temp_username_uniques AS
        SELECT username
        FROM temp_username_min_max_dt
        WHERE DATEDIFF('day', min_dt, max_dt) > 8
    """).collect()

    # --- Step 4 ---
    session.sql("""
        CREATE OR REPLACE TEMPORARY TABLE temp_dt_username_unique AS
        SELECT a.*
        FROM temp_dt_username a
        JOIN temp_username_uniques b
          ON a.username = b.username
         AND b.username IS NOT NULL
    """).collect()

    # --- Step 5 --- here I took MIN(max_version_prefix) instead of MAX to get the version that was on the first date
    session.sql("""
        CREATE OR REPLACE TEMPORARY TABLE temp_dt_username_unique_first_date AS
        SELECT username, MIN(dt) AS first_date, MIN(max_version_prefix) as max_version_prefix
        FROM temp_dt_username_unique
        GROUP BY username
        ORDER BY first_date
    """).collect()

    # --- Step 6: cohort retention ---
    # Definition: cohort = qualified users (>8 day activity span) whose first
    # observed activity falls in a given month. For each cohort we report the
    # number of users active 0..12 months later.
    #
    # Two correctness measures applied here:
    #   1. The latest `COHORT_TRAILING_HIDE_MONTHS` cohorts are filtered out:
    #      their cohort_size is unstable while late-qualifiers backfill.
    #   2. A rectangular (cohort x offset) grid is generated up to the
    #      current month and LEFT JOINed against activity, so cells with
    #      genuine zero retention are recorded as `active_users = 0` rather
    #      than silently dropped (which previously made them indistinguishable
    #      from "future month, no data yet").
    cohort_retention_df = session.sql(f"""
        WITH cohort AS (
            SELECT username,
                   DATE_TRUNC('MONTH', first_date) AS cohort_month
            FROM temp_dt_username_unique_first_date
            WHERE TO_CHAR(first_date, 'YYYY-MM') >= '{COHORT_START_MONTH}'
              AND DATE_TRUNC('MONTH', first_date) <= DATE_TRUNC(
                      'MONTH',
                      DATEADD('month', -{COHORT_TRAILING_HIDE_MONTHS}, CURRENT_DATE())
                  )
        ),
        activity AS (
            SELECT DISTINCT username,
                   DATE_TRUNC('MONTH', dt) AS active_month
            FROM temp_dt_username_unique
        ),
        sizes AS (
            SELECT cohort_month, COUNT(*) AS cohort_size
            FROM cohort
            GROUP BY 1
        ),
        offsets AS (
            -- Rectangular skeleton: one row per (cohort_month, offset 0..12),
            -- truncated to offsets that have already elapsed.
            SELECT s.cohort_month,
                   off.value::INT AS month_offset
            FROM   sizes s,
                   LATERAL FLATTEN(input => ARRAY_GENERATE_RANGE(0, 13)) off
            WHERE  DATEADD('month', off.value::INT, s.cohort_month)
                       <= DATE_TRUNC('MONTH', CURRENT_DATE())
        )
        SELECT TO_CHAR(o.cohort_month, 'YYYY-MM')                   AS cohort_month,
               o.month_offset                                       AS month_offset,
               COUNT(DISTINCT a.username)                           AS active_users,
               MAX(s.cohort_size)                                   AS cohort_size,
               ROUND(
                   100.0 * COUNT(DISTINCT a.username)
                         / NULLIF(MAX(s.cohort_size), 0),
                   2
               )                                                    AS retention_pct
        FROM   offsets o
        JOIN   sizes   s ON o.cohort_month = s.cohort_month
        LEFT JOIN cohort   c ON o.cohort_month = c.cohort_month
        LEFT JOIN activity a
               ON c.username = a.username
              AND DATEDIFF('month', o.cohort_month, a.active_month) = o.month_offset
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).to_pandas()

    # --- Final result 1: new Kedro users ---
    new_users_df = session.sql("""
        SELECT first_year_month, max_version_prefix, COUNT(*) AS count
        FROM (
            SELECT *,
                   TO_CHAR(first_date, 'YYYY-MM') AS first_year_month
            FROM temp_dt_username_unique_first_date
        ) t
        WHERE first_year_month >= '2024-11'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).to_pandas()

    # --- Final result 2: monthly active users (MAU) ---
    mau_df = session.sql("""
        SELECT 
            TO_CHAR(DATE_TRUNC('month', dt), 'YYYY-MM') AS year_month,
            max_version_prefix,
            COUNT(DISTINCT username) AS mau
        FROM temp_dt_username_unique
        WHERE dt >= '2024-10-01'
        GROUP BY year_month, max_version_prefix
        ORDER BY year_month, max_version_prefix
    """).to_pandas()

    # --- 3️⃣ Kedro plugins MAU ---
    plugins_mau_df = session.sql("""
        SELECT 
            TO_CHAR(DATE_TRUNC('month', time), 'YYYY-MM') AS year_month,
            first_two_words,
            COUNT(DISTINCT username) AS unique_users
        FROM (
            SELECT 
                a.command,
                SPLIT(command, ' ')[0] || ' ' || SPLIT(command, ' ')[1] AS first_two_words,
                b.username,
                a.time
            FROM HEAP_FRAMEWORK_VIZ_PRODUCTION.HEAP.ANY_COMMAND_RUN a
            JOIN temp_username_uniques b 
                ON a.username = b.username
            WHERE time >= '2024-10-01'
        ) t
        WHERE first_two_words IN (
            'kedro mlflow',
            'kedro docker',
            'kedro airflow',
            'kedro databricks',
            'kedro azureml',
            'kedro vertexai',
            'kedro gql',
            'kedro boot',
            'kedro sagemaker',
            'kedro coda',
            'kedro kubeflow'
        )
        GROUP BY year_month, first_two_words
        ORDER BY year_month, unique_users DESC
    """).to_pandas()

    # --- 4️⃣ Kedro core commands MAU ---
    commands_mau_df = session.sql("""
        SELECT 
            TO_CHAR(DATE_TRUNC('month', time), 'YYYY-MM') AS year_month,
            first_two_words,
            COUNT(DISTINCT username) AS unique_users
        FROM (
            SELECT 
                a.command,
                SPLIT(command, ' ')[0] || ' ' || SPLIT(command, ' ')[1] AS first_two_words,
                b.username,
                a.time
            FROM HEAP_FRAMEWORK_VIZ_PRODUCTION.HEAP.ANY_COMMAND_RUN a
            JOIN temp_username_uniques b 
                ON a.username = b.username
            WHERE time >= '2024-10-01'
        ) t
        WHERE first_two_words IN (
            'kedro run',
            'kedro viz',
            'kedro new',
            'kedro pipeline',
            'kedro jupyter',
            'kedro ipython',
            'kedro package'
        )
        GROUP BY year_month, first_two_words
        ORDER BY year_month, unique_users DESC
    """).to_pandas()

    session.close()
    return new_users_df, mau_df, plugins_mau_df, commands_mau_df, cohort_retention_df
