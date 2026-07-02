import ibis
import ibis.expr.types as ir

# Lower-cased prefixes of the telemetry `dataset_type_count.<FQN>` columns we track
# (kedro-telemetry >= 0.8). Matching by prefix picks up new datasets automatically.
_DATASET_COUNT_PREFIXES = (
    "dataset_type_count_kedro_datasets_langchain_",  # core langchain (all GenAI)
    "dataset_type_count_kedro_datasets_experimental_",  # everything experimental
)

# Experimental sub-packages that are GenAI/LLM-related; the rest is MLOps/data-format.
_GENAI_PACKAGES = ["chromadb", "langchain", "langfuse", "opik"]

# Real release versions only (e.g. 0.19, 1.2.6); excludes "test", "dev", "main".
_RELEASE_VERSION_RE = r"^[0-9]+[.][0-9].*$"


def _real_release_events(t: ir.Table) -> ir.Table:
    """Keep only real user events on a released version: not CI, a real release,
    and not the never-released 0.20 (which only comes from pre-release installs)."""
    return t.filter(
        t.is_ci_env.isnull() | (t.is_ci_env == "false"),
        t.project_version.rlike(_RELEASE_VERSION_RE),
        ~t.project_version.startswith("0.20"),
    )


def aggregate_project_stats(heap_stats: ir.Table) -> ir.Table:
    """Aggregate raw events to one row per (username, day) since 2024-09-01."""
    heap_stats = _real_release_events(heap_stats.rename(str.lower))
    return (
        heap_stats.filter(heap_stats.time.date() >= ibis.date("2024-09-01"))
        .group_by(["username", heap_stats.time.date().name("dt")])
        .agg(max_version_prefix=heap_stats.project_version.left(4).max())
    )


def get_unique_users(dt_username: ir.Table) -> ir.Table:
    """Filter to users whose activity spans more than 8 days (max_dt - min_dt > 8)."""
    min_max = dt_username.group_by("username").agg(
        min_dt=dt_username.dt.min(), max_dt=dt_username.dt.max()
    )
    return min_max.filter(
        min_max.max_dt > min_max.min_dt + ibis.interval(days=8)
    ).select("username")


def get_active_events(dt_username: ir.Table, unique_users: ir.Table) -> ir.Table:
    """Filter aggregated events to the unique-user list."""
    return dt_username.join(unique_users, "username")[dt_username.columns]


def build_new_users_monthly(active_events: ir.Table) -> ir.Table:
    """Count new Kedro users per month (first seen since 2024-11)."""
    first_dates = active_events.group_by("username").agg(
        first_date=ibis._.dt.min(),
        max_version_prefix=ibis._.max_version_prefix.min(),
    )
    return (
        first_dates.mutate(first_year_month=ibis._.first_date.strftime("%Y-%m"))
        .filter(ibis._.first_year_month >= "2024-11")
        .group_by(["first_year_month", "max_version_prefix"])
        .agg(count=ibis._.count())
        .order_by(["first_year_month", "max_version_prefix"])
    )


def build_mau(active_events: ir.Table) -> ir.Table:
    """Count monthly active unique users by version since 2024-10."""
    return (
        active_events.filter(ibis._.dt >= ibis.date("2024-10-01"))
        .mutate(year_month=ibis._.dt.truncate("M").strftime("%Y-%m"))
        .group_by(["year_month", "max_version_prefix"])
        .agg(mau=ibis._.username.nunique())
        .order_by(["year_month", "max_version_prefix"])
    )


def build_cohort_retention(
    active_events: ir.Table,
    cohort_trailing_hide_months: int,
) -> ir.Table:
    """Cohort × month retention matrix; zero-retention cells are explicit, not missing."""
    first_dates = (
        active_events.group_by("username")
        .agg(first_date=active_events.dt.min())
        .mutate(cohort_month=lambda t: t.first_date.truncate("M"))
    )

    cutoff_month = ibis.now().date().truncate("M") - ibis.interval(
        months=cohort_trailing_hide_months
    )

    cohort = first_dates.filter(
        [
            # Earliest cohort with full Heap telemetry coverage.
            first_dates.cohort_month.strftime("%Y-%m") >= "2024-11",
            first_dates.cohort_month <= cutoff_month,
        ]
    ).select("username", "cohort_month")

    # Rename to disambiguate after left join: active_username is NULL for non-active rows,
    # which makes COUNT(DISTINCT active_username) naturally give 0 for those cells.
    activity = (
        active_events.select("username", active_month=active_events.dt.truncate("M"))
        .distinct()
        .rename(active_username="username")
    )

    sizes = cohort.group_by("cohort_month").agg(cohort_size=ibis._.count())

    # Rectangular (cohort × month offset) grid, clipped to elapsed months.
    # ibis.range(0, 13).unnest() compiles to LATERAL FLATTEN(ARRAY_GENERATE_RANGE(0, 13))
    # on Snowflake, producing one row per offset 0–12 per cohort month.
    today = ibis.now().date()
    offsets = (
        sizes.mutate(month_offset=ibis.range(0, 13))
        .unnest("month_offset")
        .filter(
            lambda t: (
                (today.year() - t.cohort_month.year()) * 12
                + (today.month() - t.cohort_month.month())
                >= t.month_offset
            )
        )
    )

    # Expand grid to per-user rows, then left-join activity at the target month.
    # Month difference via year/month components mirrors DATEDIFF('month', ...).
    expanded = offsets.join(cohort, "cohort_month")
    month_diff = (activity.active_month.year() - expanded.cohort_month.year()) * 12 + (
        activity.active_month.month() - expanded.cohort_month.month()
    )

    return (
        expanded.left_join(
            activity,
            (expanded.username == activity.active_username)
            & (month_diff == expanded.month_offset),
        )
        .mutate(cohort_month_str=lambda t: t.cohort_month.strftime("%Y-%m"))
        .group_by(["cohort_month_str", "month_offset"])
        .agg(
            active_users=lambda t: t.active_username.nunique(),
            cohort_size=lambda t: t.cohort_size.max(),
        )
        .rename(cohort_month="cohort_month_str")
        .mutate(
            retention_pct=(
                100.0 * ibis._.active_users / ibis._.cohort_size.nullif(0)
            ).round(2)
        )
        .order_by(["cohort_month", "month_offset"])
    )


def build_command_mau(
    any_command_run: ir.Table, unique_users: ir.Table, keep_prefixes: list[str]
) -> ir.Table:
    """Count monthly unique users per command, filtered to the given two-word prefixes."""
    any_command_run = any_command_run.rename(str.lower)
    words = any_command_run.command.split(" ")
    base = (
        any_command_run.join(unique_users, "username")[any_command_run.columns]
        .filter(ibis._.time >= ibis.timestamp("2024-10-01"))
        .mutate(first_two_words=words[0].concat(" ").concat(words[1]))
    )
    return (
        base.filter(ibis._.first_two_words.isin(keep_prefixes))
        .mutate(year_month=ibis._.time.truncate("M").strftime("%Y-%m"))
        .group_by(["year_month", "first_two_words"])
        .agg(unique_users=ibis._.username.nunique())
        .order_by(["year_month", ibis.desc("unique_users")])
    )


def _genai_experimental_long(heap_project_statistics: ir.Table) -> ir.Table:
    """Unpivot the wide ``dataset_type_count_*`` columns to one labelled row per
    (event, dataset class), keeping only core-langchain and experimental datasets."""
    # Same real-release filter as the MAU/cohort metrics, so the numbers line up.
    t = _real_release_events(heap_project_statistics.rename(str.lower))

    count_cols = [c for c in t.columns if c.startswith(_DATASET_COUNT_PREFIXES)]
    if not count_cols:
        # No dataset-count columns yet (older table or changed schema). Add an
        # empty placeholder so the long-form expression returns zero rows instead
        # of raising.
        placeholder = _DATASET_COUNT_PREFIXES[0] + "none"
        t = t.mutate(**{placeholder: ibis.literal(None, type="string")})
        count_cols = [placeholder]
    t = t.select("username", "time", *count_cols)

    # Counts are TEXT and only emitted when >= 1. Avoid `pivot_longer()` here:
    # on Snowflake, Ibis 12 expands it through nested FLATTEN/object expressions
    # that are much slower than explicit UNION ALL branches for this small
    # tracked-column set.
    long_parts = []
    for count_col in count_cols:
        positive_count = t[count_col].rlike(r"^[1-9][0-9]*$")
        count_events = t.filter(positive_count)
        long_parts.append(
            count_events.select(
                "username",
                "time",
                ibis.literal(count_col).name("ds_class"),
                count_events[count_col].cast("int").name("ds_count"),
            )
        )

    long = ibis.union(*long_parts, distinct=False)

    ds = long.ds_class
    is_experimental = ds.contains("kedro_datasets_experimental_")
    package = is_experimental.ifelse(
        ds.re_extract(r"kedro_datasets_experimental_([a-z0-9]+)", 1), "langchain"
    )
    return long.mutate(
        event=long.username + "|" + long.time.cast("string"),
        month=long.time.truncate("M").cast("date"),
        namespace=is_experimental.ifelse("experimental", "core"),
        is_genai=~is_experimental | package.isin(_GENAI_PACKAGES),
        tool=ibis.cases(
            (package == "chromadb", "ChromaDB (vector store)"),
            (package == "langfuse", "Langfuse (LLM observability)"),
            (package == "opik", "Opik (LLM observability)"),
            ((package == "langchain") & is_experimental, "LangChain prompt"),
            (
                (package == "langchain") & ~is_experimental,
                "LangChain (chat / embeddings)",
            ),
            else_=package,  # other experimental datasets show their package name
        ),
        dataset_class=ds.replace("dataset_type_count_", ""),
    )


def _usage_agg(grouped, *, dedup_runs: bool, with_dates: bool) -> ir.Table:
    """Standard usage metrics for a grouped (or whole) GenAI/experimental table.

    ``dedup_runs`` controls how ``project_runs`` is counted: the per-dataset grain
    has exactly one row per event so ``count()`` is exact; the per-tool/roll-up
    grains span several datasets per event and must de-duplicate (``event.nunique()``).
    """
    aggs = {
        "unique_users": ibis._.username.nunique(),
        "project_runs": ibis._.event.nunique() if dedup_runs else ibis._.count(),
        "total_catalog_entries": ibis._.ds_count.sum(),
    }
    if with_dates:
        aggs["first_seen"] = ibis._.time.min().cast("date")
        aggs["last_seen"] = ibis._.time.max().cast("date")
    return grouped.agg(**aggs)


def build_experimental_dataset_usage(
    heap_project_statistics: ir.Table,
    genai_min_users: int,
) -> tuple[ir.Table, ir.Table, ir.Table]:
    """Usage of GenAI/experimental datasets, at per-dataset and per-tool grains.

    Returns ``(per-dataset monthly, per-dataset summary, per-tool summary)``. The
    per-tool summary collapses a tool's datasets (e.g. Langfuse Prompt/Trace/
    Evaluation) into one row with *de-duplicated* distinct users - the figure the
    dashboard's left chart shows.

    The dashboard filters ``is_genai`` (GenAI page) or ``namespace == 'experimental'``
    (all-experimental page). Rows below ``genai_min_users`` distinct users are
    suppressed (k-anonymity); the per-dataset ALL-* roll-up rows are exempt.
    """
    long = _genai_experimental_long(heap_project_statistics)
    group_keys = ["namespace", "is_genai", "tool", "dataset_class"]

    monthly = (
        _usage_agg(
            long.group_by(["month", *group_keys]), dedup_runs=False, with_dates=False
        )
        .filter(ibis._.unique_users >= genai_min_users)
        .order_by(["month", "namespace", ibis.desc("unique_users")])
    )

    summary_cols = [
        *group_keys,
        "unique_users",
        "project_runs",
        "total_catalog_entries",
        "first_seen",
        "last_seen",
    ]
    per_dataset = (
        _usage_agg(long.group_by(group_keys), dedup_runs=False, with_dates=True)
        .filter(ibis._.unique_users >= genai_min_users)
        .select(summary_cols)
    )

    def _rollup(
        subset: ir.Table, label: str, ns: str, is_genai: bool | None
    ) -> ir.Table:
        # De-duplicated group totals (the grouped datasets share events, so runs
        # are counted by distinct event = username + time).
        return (
            _usage_agg(subset, dedup_runs=True, with_dates=True)
            .mutate(
                namespace=ibis.literal(ns),
                is_genai=ibis.literal(is_genai, type="boolean"),
                tool=ibis.literal(label),
                dataset_class=ibis.literal(label),
            )
            .select(summary_cols)
        )

    summary = (
        per_dataset.union(
            _rollup(long.filter(long.is_genai), "ALL GenAI datasets", "genai", True)
        )
        # is_genai is NULL (not False): this total mixes GenAI and non-GenAI
        # experimental datasets, so it must not be read as "non-GenAI".
        .union(
            _rollup(
                long.filter(long.namespace == "experimental"),
                "ALL experimental datasets",
                "experimental",
                None,
            )
        )
        # Drop empty roll-ups (e.g. no GenAI/experimental columns yet) so the
        # output is genuinely empty rather than zeroed totals that look like data.
        .filter(ibis._.unique_users > 0)
        .order_by([ibis.desc("is_genai"), ibis.desc("unique_users")])
    )

    # Per-tool grain: a tool's datasets collapse into one
    # row with de-duplicated distinct users / runs; total_catalog_entries stays
    # additive. (Per-tool *monthly* isn't needed - the monthly chart shows the
    # additive catalog-entries metric, which the dashboard sums from `monthly`.)
    tool_summary = (
        _usage_agg(
            long.group_by(["namespace", "is_genai", "tool"]),
            dedup_runs=True,
            with_dates=True,
        )
        .filter(ibis._.unique_users >= genai_min_users)
        .order_by([ibis.desc("is_genai"), ibis.desc("unique_users")])
    )

    return monthly, summary, tool_summary
