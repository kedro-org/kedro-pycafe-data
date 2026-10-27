import ibis.expr.types as ir
import pandas as pd


def identity(tbl: ir.Table) -> ir.Table:
    return tbl


def download_segments(downloads: ir.Table) -> ir.Table:
    """Aggregate the investigated traffic profile using SELECT expressions only."""
    downloads = downloads.filter(
        downloads.project.isin(["kedro", "kedro-viz"]),
        downloads.DATE >= "2020-01-01",  # Same history as V_PYPI_KEDRO_DOWNLOADS.
    )
    in_group = (
        (downloads.country_code == "US")
        & (downloads.installer_name == "pip")
        & (downloads.system_name == "Linux")
        & (downloads.system_release_cat == "azure")
        & downloads.python.like("3.12.%")
        & (downloads.python != "3.12.3")
        & ~downloads.ci.fill_null(False)
    ).fill_null(False)
    rows = downloads.select(
        PROJECT=downloads.project,
        DATE=downloads.DATE,
        SEGMENT=in_group.ifelse("likely_automated", "other"),
        COUNT=downloads["count"],
    )
    return rows.group_by("PROJECT", "DATE", "SEGMENT").aggregate(
        COUNT=rows.COUNT.sum(),
        INVALID_COUNTS=(rows.COUNT.isnull() | (rows.COUNT < 0)).sum(),
    )


def _validate_counts(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if frame.empty or frame[keys + ["COUNT"]].isna().any().any():
        raise ValueError("Download export is empty or contains missing values.")
    frame = frame.copy()
    frame["DATE"] = pd.to_datetime(frame["DATE"]).dt.date
    frame["COUNT"] = pd.to_numeric(frame["COUNT"], errors="raise")
    if (frame["COUNT"] < 0).any() or (frame["COUNT"] % 1 != 0).any():
        raise ValueError("Download counts must be nonnegative whole numbers.")
    if frame.duplicated(keys).any():
        raise ValueError("Download export contains duplicate keys.")
    frame["COUNT"] = frame["COUNT"].astype("int64")
    return frame.sort_values(keys).reset_index(drop=True)


def export_downloads(
    totals: ir.Table, extended: ir.Table
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read and reconcile both exports before either CSV is saved."""
    daily = _validate_counts(
        totals.select("PROJECT", "DATE", "COUNT").execute(), ["PROJECT", "DATE"]
    )
    segments = download_segments(extended).execute()
    if segments["INVALID_COUNTS"].ne(0).any():
        raise ValueError("Extended downloads contain missing or negative counts.")
    segments = _validate_counts(
        segments.drop(columns="INVALID_COUNTS"), ["PROJECT", "DATE", "SEGMENT"]
    )
    split_totals = segments.groupby(["PROJECT", "DATE"], as_index=False)["COUNT"].sum()
    comparison = daily.merge(
        split_totals,
        on=["PROJECT", "DATE"],
        how="outer",
        suffixes=("_main", "_split"),
        validate="one_to_one",
        indicator=True,
    )
    if (
        comparison["_merge"].ne("both")
        | comparison["COUNT_main"].ne(comparison["COUNT_split"])
    ).any():
        raise ValueError(
            "Main and extended download totals differ. "
            "Refresh both source tables before publishing the CSVs."
        )
    return daily, segments
