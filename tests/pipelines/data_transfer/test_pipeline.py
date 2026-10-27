from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import ibis
import pandas as pd
import pytest
import yaml
from kedro.io import DataCatalog, MemoryDataset
from kedro.runner import SequentialRunner
from kedro_datasets.pandas import CSVDataset

from kedro_pycafe_data.pipelines.data_transfer import nodes
from kedro_pycafe_data.pipelines.data_transfer.nodes import (
    download_segments,
    export_downloads,
)
from kedro_pycafe_data.pipelines.data_transfer.pipeline import create_pipeline

DAY = date(2026, 5, 1)
SCHEMA = {
    "project": "string",
    "DATE": "date",
    "country_code": "string",
    "installer_name": "string",
    "system_name": "string",
    "system_release_cat": "string",
    "python": "string",
    "ci": "boolean",
    "count": "int64",
}


def traffic(**changes):
    return {
        "project": "kedro-viz",
        "DATE": DAY,
        "country_code": "US",
        "installer_name": "pip",
        "system_name": "Linux",
        "system_release_cat": "azure",
        "python": "3.12.13",
        "ci": None,
        "count": 10,
        **changes,
    }


@pytest.fixture(autouse=True)
def export_clock(monkeypatch):
    clock = Mock(wraps=datetime)
    clock.now.return_value = datetime(2026, 5, 3, tzinfo=timezone.utc)
    monkeypatch.setattr(nodes, "datetime", clock)
    return clock


@pytest.fixture
def connection():
    con = ibis.duckdb.connect()
    yield con
    con.disconnect()


def tables(connection, rows, daily=None):
    extended = connection.create_table("extended", pd.DataFrame(rows), schema=SCHEMA)
    if daily is None:
        daily = [{"PROJECT": "kedro-viz", "DATE": DAY, "COUNT": 10}]
    totals = connection.create_table("totals", pd.DataFrame(daily))
    return totals, extended


@pytest.mark.parametrize(
    "changes,segment",
    [
        ({}, "likely_automated"),
        ({"ci": False}, "likely_automated"),
        ({"ci": True}, "other"),
        ({"python": "3.12.3"}, "other"),
        ({"python": "3.11.13"}, "other"),
        ({"python": None}, "other"),
        ({"country_code": "NL"}, "other"),
        ({"country_code": None}, "other"),
        ({"installer_name": "uv"}, "other"),
        ({"system_name": "Windows"}, "other"),
        ({"system_release_cat": "aws"}, "other"),
        ({"system_release_cat": None}, "other"),
    ],
)
def test_traffic_classification(connection, changes, segment):
    totals, extended = tables(connection, [traffic(**changes)])
    daily, split = export_downloads(totals, extended)
    assert split.to_dict("records") == [
        {"PROJECT": "kedro-viz", "DATE": DAY, "SEGMENT": segment, "COUNT": 10}
    ]
    assert daily["COUNT"].sum() == split["COUNT"].sum()


def test_preserves_totals_and_existing_history_window(connection):
    totals, extended = tables(
        connection,
        [
            traffic(count=4),
            traffic(count=6, python="3.12.14"),
            traffic(count=3, ci=True),
            traffic(count=2, installer_name="uv"),
            traffic(project="kedro", count=7),
            traffic(project="kedro-datasets", count=99),
            traffic(DATE=date(2019, 12, 31), count=99),
        ],
        [
            {"PROJECT": "kedro-viz", "DATE": DAY, "COUNT": 15},
            {"PROJECT": "kedro", "DATE": DAY, "COUNT": 7},
        ],
    )
    daily, split = export_downloads(totals, extended)
    assert list(daily.columns) == ["PROJECT", "DATE", "COUNT"]
    assert list(split.columns) == ["PROJECT", "DATE", "SEGMENT", "COUNT"]
    assert split["COUNT"].tolist() == [7, 10, 5]
    assert not split.duplicated(["PROJECT", "DATE", "SEGMENT"]).any()
    assert daily["COUNT"].sum() == split["COUNT"].sum() == 22


@pytest.mark.parametrize("bad_count", [None, -1])
def test_rejects_bad_source_counts_even_if_aggregate_is_positive(connection, bad_count):
    totals, extended = tables(connection, [traffic(count=11), traffic(count=bad_count)])
    with pytest.raises(ValueError, match="missing or negative"):
        export_downloads(totals, extended)


@pytest.mark.parametrize(
    "daily",
    [
        [{"PROJECT": "kedro-viz", "DATE": DAY, "COUNT": 11}],
        [{"PROJECT": "kedro-viz", "DATE": date(2026, 5, 2), "COUNT": 10}],
        [{"PROJECT": "kedro", "DATE": DAY, "COUNT": 10}],
    ],
)
def test_rejects_different_counts_or_coverage(connection, daily):
    totals, extended = tables(connection, [traffic()], daily)
    with pytest.raises(ValueError, match="totals differ"):
        export_downloads(totals, extended)


def test_rejects_duplicate_daily_keys(connection):
    row = {"PROJECT": "kedro-viz", "DATE": DAY, "COUNT": 5}
    totals, extended = tables(connection, [traffic()], [row, row])
    with pytest.raises(ValueError, match="duplicate keys"):
        export_downloads(totals, extended)


def test_snowflake_expression_compiles():
    query = download_segments(ibis.table(SCHEMA, name="AGGREGATED_DOWNLOADS_EXT"))
    sql = ibis.to_sql(query, dialect="snowflake")
    assert sql.lstrip().startswith("SELECT")


@pytest.mark.parametrize("main_count", [10, 11])
def test_pipeline_saves_only_validated_csvs(connection, tmp_path, main_count):
    totals, extended = tables(
        connection,
        [traffic(count=7), traffic(count=3, ci=True)],
        [{"PROJECT": "kedro-viz", "DATE": DAY, "COUNT": main_count}],
    )
    config_path = Path(__file__).parents[3] / "conf/base/catalog.yml"
    config = yaml.safe_load(config_path.read_text())
    catalog = DataCatalog()
    catalog["pypi_kedro_downloads"] = MemoryDataset(totals, copy_mode="assign")
    catalog["pypi_kedro_downloads_extended"] = MemoryDataset(
        extended, copy_mode="assign"
    )
    for name in ["pypi_kedro_downloads_table", "pypi_kedro_downloads_split_table"]:
        output = config[name]
        assert output["type"] == "pandas.CSVDataset"
        catalog[name] = CSVDataset(
            filepath=str(tmp_path / Path(output["filepath"]).name),
            save_args=output["save_args"],
        )
    pipeline = create_pipeline().only_nodes("fetch_and_save_snowflake_data")
    if main_count != 10:
        with pytest.raises(ValueError, match="totals differ"):
            SequentialRunner().run(pipeline, catalog)
        assert not list(tmp_path.glob("*.csv"))
        return
    SequentialRunner().run(pipeline, catalog)
    daily = pd.read_csv(tmp_path / "pypi_kedro_downloads.csv")
    split = pd.read_csv(tmp_path / "pypi_kedro_downloads_split.csv")
    assert daily.to_dict("records") == [
        {"PROJECT": "kedro-viz", "DATE": "2026-05-01", "COUNT": 10}
    ]
    assert split["SEGMENT"].tolist() == ["likely_automated", "other"]
    assert split["COUNT"].tolist() == [7, 3]


@pytest.mark.parametrize("partial_source", ["main", "extended", "both"])
def test_excludes_unfinished_and_future_days_from_both_exports(
    connection, export_clock, partial_source
):
    rows = [traffic()]
    daily_rows = [{"PROJECT": "kedro-viz", "DATE": DAY, "COUNT": 10}]
    for day in [date(2026, 5, 3), date(2026, 5, 4)]:
        if partial_source in ["main", "both"]:
            daily_rows.append({"PROJECT": "kedro-viz", "DATE": day, "COUNT": 99})
        if partial_source in ["extended", "both"]:
            rows.append(traffic(DATE=day, count=50))
    totals, extended = tables(connection, rows, daily_rows)
    daily, split = export_downloads(totals, extended)
    assert daily["DATE"].tolist() == [DAY]
    assert split["DATE"].tolist() == [DAY]
    assert daily["COUNT"].tolist() == split["COUNT"].tolist() == [10]
    export_clock.now.assert_called_once_with(timezone.utc)


@pytest.mark.parametrize("missing_source", ["main", "extended"])
def test_does_not_drop_completed_dates_missing_from_one_table(
    connection, missing_source
):
    yesterday = date(2026, 5, 2)
    rows = [traffic()]
    daily_rows = [{"PROJECT": "kedro-viz", "DATE": DAY, "COUNT": 10}]
    if missing_source == "main":
        rows.append(traffic(DATE=yesterday))
    else:
        daily_rows.append({"PROJECT": "kedro-viz", "DATE": yesterday, "COUNT": 10})
    totals, extended = tables(connection, rows, daily_rows)
    with pytest.raises(ValueError, match="totals differ"):
        export_downloads(totals, extended)
