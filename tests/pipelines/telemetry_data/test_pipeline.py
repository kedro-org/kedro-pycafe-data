import logging
from pathlib import Path

import pandas as pd
import pytest
from kedro.io import DataCatalog
from kedro.runner import SequentialRunner
from kedro_datasets.ibis import FileDataset

from kedro_pycafe_data.pipelines.telemetry_data.pipeline import create_pipeline

DATA_DIR = Path(__file__).parent

PLUGINS = [
    "kedro mlflow",
    "kedro docker",
    "kedro airflow",
    "kedro databricks",
    "kedro azureml",
    "kedro vertexai",
    "kedro gql",
    "kedro boot",
    "kedro sagemaker",
    "kedro coda",
    "kedro kubeflow",
]

COMMANDS = [
    "kedro run",
    "kedro viz",
    "kedro new",
    "kedro pipeline",
    "kedro jupyter",
    "kedro ipython",
    "kedro package",
]


@pytest.fixture
def catalog():
    catalog = DataCatalog()
    catalog["heap_project_statistics"] = FileDataset(
        filepath=str(DATA_DIR / "heap_project_statistics.csv"),
        file_format="csv",
        load_args={
            "columns": {
                "time": "TIMESTAMP",
                "username": "VARCHAR",
                "is_ci_env": "VARCHAR",
                "project_version": "VARCHAR",
                # Heap stores dataset-count properties as TEXT.
                "dataset_type_count_kedro_datasets_langchain_chat_openai_dataset_chatopenaidataset": "VARCHAR",
                "dataset_type_count_kedro_datasets_experimental_langfuse_trace_dataset_tracedataset": "VARCHAR",
                "dataset_type_count_kedro_datasets_experimental_mlrun_model_mlrunmodel": "VARCHAR",
            }
        },
    )
    catalog["heap_any_command_run"] = FileDataset(
        filepath=str(DATA_DIR / "heap_any_command_run.csv"),
        file_format="csv",
    )
    catalog["params:plugins"] = PLUGINS
    catalog["params:commands"] = COMMANDS
    catalog["params:cohort_trailing_hide_months"] = 2
    catalog["params:genai_min_users"] = 1
    return catalog


def test_telemetry_pipeline(catalog, caplog):
    with caplog.at_level(logging.DEBUG):
        SequentialRunner().run(create_pipeline(), catalog)

    assert "Pipeline execution completed successfully" in caplog.text

    # user_a (first seen 2024-09) is not a "new user" (filter: first_year_month >= 2024-11)
    # user_b (first seen 2024-11) and user_c (first seen 2024-12) are
    new_users = catalog.load("new_kedro_users_monthly").execute()
    assert set(new_users.columns) == {"first_year_month", "max_version_prefix", "count"}
    assert set(new_users["first_year_month"].tolist()) == {"2024-11", "2024-12"}

    # user_a (0.19.x) active in 2024-10; user_b (0.18/1.0) in 2024-11; user_c (1.0) in 2024-12
    # user_short excluded: only 4-day activity span, does not meet the >8-day unique-user filter
    mau = catalog.load("mau_kedro").execute()
    assert set(mau.columns) == {"year_month", "max_version_prefix", "mau"}
    assert set(mau["year_month"].tolist()) == {
        "2024-10",
        "2024-11",
        "2024-12",
        "2025-01",
    }
    # user_test_020 (only 0.20.0, active 2025-02) is dropped: 0.20 was never released.
    # Its month would otherwise appear above and its prefix here.
    assert "0.20" not in mau["max_version_prefix"].tolist()

    commands_mau = catalog.load("kedro_commands_mau").execute()
    assert set(commands_mau.columns) == {
        "year_month",
        "first_two_words",
        "unique_users",
    }
    assert "kedro run" in commands_mau["first_two_words"].tolist()
    assert "kedro pipeline" in commands_mau["first_two_words"].tolist()

    plugins_mau = catalog.load("kedro_plugins_mau").execute()
    assert set(plugins_mau.columns) == {"year_month", "first_two_words", "unique_users"}
    assert "kedro mlflow" in plugins_mau["first_two_words"].tolist()

    # Cohort filter: cohort_month >= 2024-11; user_a cohort (2024-09) excluded
    retention = catalog.load("cohort_retention").execute()
    assert set(retention.columns) == {
        "cohort_month",
        "month_offset",
        "active_users",
        "cohort_size",
        "retention_pct",
    }
    assert set(retention["cohort_month"].tolist()) == {"2024-11", "2024-12"}
    # Offset 0 is always 100% retention (cohort_size users active in their first month)
    assert (retention[retention["month_offset"] == 0]["retention_pct"] == 100.0).all()

    # GenAI / experimental dataset usage. user_ci (is_ci_env=true, ChatOpenAI=5)
    # is excluded by the CI filter, so only user_b's ChatOpenAI runs count.
    usage = catalog.load("experimental_dataset_usage_summary").execute()
    assert set(usage.columns) == {
        "namespace",
        "is_genai",
        "tool",
        "dataset_class",
        "unique_users",
        "project_runs",
        "total_catalog_entries",
        "first_seen",
        "last_seen",
    }
    usage = usage.set_index("dataset_class")

    chat = usage.loc["kedro_datasets_langchain_chat_openai_dataset_chatopenaidataset"]
    assert chat["is_genai"] and chat["namespace"] == "core"
    assert int(chat["unique_users"]) == 1 and int(chat["total_catalog_entries"]) == 3

    langfuse = usage.loc["kedro_datasets_experimental_langfuse_trace_dataset_tracedataset"]
    assert langfuse["is_genai"] and langfuse["namespace"] == "experimental"

    mlrun = usage.loc["kedro_datasets_experimental_mlrun_model_mlrunmodel"]
    assert not mlrun["is_genai"] and mlrun["namespace"] == "experimental"

    # Roll-ups are de-duplicated across datasets (user_b, user_c over 3 distinct runs).
    genai_all = usage.loc["ALL GenAI datasets"]
    assert int(genai_all["unique_users"]) == 2 and int(genai_all["project_runs"]) == 3
    # The all-experimental total mixes GenAI and non-GenAI, so is_genai must be NULL.
    assert int(usage.loc["ALL experimental datasets", "unique_users"]) == 2
    assert pd.isna(usage.loc["ALL experimental datasets", "is_genai"])

    monthly = catalog.load("experimental_dataset_usage_monthly").execute()
    assert set(monthly.columns) == {
        "month",
        "namespace",
        "is_genai",
        "tool",
        "dataset_class",
        "unique_users",
        "project_runs",
        "total_catalog_entries",
    }
    monthly["month_str"] = pd.to_datetime(monthly["month"]).dt.strftime("%Y-%m-%d")

    # ChatOpenAI only appears in 2024-11, and only user_b's 2 runs survive the CI
    # filter (user_ci's count of 5 is dropped) -> guards month grouping + CI filter.
    chat = monthly[
        monthly["dataset_class"]
        == "kedro_datasets_langchain_chat_openai_dataset_chatopenaidataset"
    ]
    assert len(chat) == 1
    assert chat.iloc[0]["month_str"] == "2024-11-01"
    assert int(chat.iloc[0]["unique_users"]) == 1
    assert int(chat.iloc[0]["project_runs"]) == 2
    assert int(chat.iloc[0]["total_catalog_entries"]) == 3

    # Langfuse trace spans two months -> guards month truncation into distinct rows.
    langfuse_months = set(
        monthly[
            monthly["dataset_class"]
            == "kedro_datasets_experimental_langfuse_trace_dataset_tracedataset"
        ]["month_str"]
    )
    assert langfuse_months == {"2024-11-01", "2024-12-01"}
