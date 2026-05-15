import logging
from pathlib import Path

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
    assert set(mau["year_month"].tolist()) == {"2024-10", "2024-11", "2024-12", "2025-01"}

    commands_mau = catalog.load("kedro_commands_mau").execute()
    assert set(commands_mau.columns) == {"year_month", "first_two_words", "unique_users"}
    assert "kedro run" in commands_mau["first_two_words"].tolist()
    assert "kedro pipeline" in commands_mau["first_two_words"].tolist()

    plugins_mau = catalog.load("kedro_plugins_mau").execute()
    assert set(plugins_mau.columns) == {"year_month", "first_two_words", "unique_users"}
    assert "kedro mlflow" in plugins_mau["first_two_words"].tolist()

    # Cohort filter: cohort_month >= 2024-11; user_a cohort (2024-09) excluded
    retention = catalog.load("cohort_retention").execute()
    assert set(retention.columns) == {
        "cohort_month", "month_offset", "active_users", "cohort_size", "retention_pct"
    }
    assert set(retention["cohort_month"].tolist()) == {"2024-11", "2024-12"}
    # Offset 0 is always 100% retention (cohort_size users active in their first month)
    assert (retention[retention["month_offset"] == 0]["retention_pct"] == 100.0).all()
