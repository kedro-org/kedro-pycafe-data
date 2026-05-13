import ibis
import ibis.expr.types as ir


def aggregate_project_stats(heap_stats: ir.Table) -> ir.Table:
    """Aggregate raw events to one row per (username, day) since 2024-09-01."""
    return (
        heap_stats
        .filter([
            heap_stats.time.date() >= ibis.date("2024-09-01"),
            heap_stats.is_ci_env.isnull() | (heap_stats.is_ci_env == "false"),
            # Keep only real release versions (e.g. 0.19, 1.2.6); drops "test", "dev", "main"
            heap_stats.project_version.rlike(r"^[0-9]+[.][0-9].*$"),
        ])
        .group_by(["username", heap_stats.time.date().name("dt")])
        .agg(max_version_prefix=heap_stats.project_version.left(4).max())
    )


def get_unique_users(dt_username: ir.Table) -> ir.Table:
    """Filter to users whose activity spans more than 8 days (max_dt - min_dt > 8)."""
    min_max = dt_username.group_by("username").agg(
        min_dt=dt_username.dt.min(), max_dt=dt_username.dt.max()
    )
    return (
        min_max
        .filter((min_max.max_dt - min_max.min_dt) > ibis.interval(days=8))
        .select("username")
    )


def get_active_events(dt_username: ir.Table, unique_users: ir.Table) -> ir.Table:
    """Filter aggregated events to the unique-user list."""
    return dt_username.join(unique_users, "username")[dt_username.columns]


def build_new_users_monthly(active_events: ir.Table) -> ir.Table:
    """Count new Kedro users per month (first seen since 2024-11)."""
    first_dates = (
        active_events.group_by("username")
        .agg(
            first_date=active_events.dt.min(),
            max_version_prefix=active_events.max_version_prefix.min(),
        )
    )
    return (
        first_dates
        .mutate(first_year_month=first_dates.first_date.strftime("%Y-%m"))
        .filter(lambda t: t.first_year_month >= "2024-11")
        .group_by(["first_year_month", "max_version_prefix"])
        .agg(count=ibis._.count())
        .order_by(["first_year_month", "max_version_prefix"])
        .rename(str.upper)
    )


def build_mau(active_events: ir.Table) -> ir.Table:
    """Count monthly active unique users by version since 2024-10."""
    return (
        active_events
        .filter(active_events.dt >= ibis.date("2024-10-01"))
        .mutate(year_month=active_events.dt.truncate("M").strftime("%Y-%m"))
        .group_by(["year_month", "max_version_prefix"])
        .agg(mau=active_events.username.nunique())
        .order_by(["year_month", "max_version_prefix"])
        .rename(str.upper)
    )


def build_cohort_retention(
    active_events: ir.Table,
    cohort_start_month: str,
    cohort_trailing_hide_months: int,
) -> ir.Table:
    """Cohort × month retention matrix; zero-retention cells are explicit, not missing."""
    first_dates = (
        active_events.group_by("username")
        .agg(first_date=active_events.dt.min())
        .mutate(cohort_month=lambda t: t.first_date.truncate("M"))
    )

    cutoff_month = (
        ibis.now().date().truncate("M")
        - ibis.interval(months=cohort_trailing_hide_months)
    )

    cohort = (
        first_dates
        .filter([
            first_dates.cohort_month.strftime("%Y-%m") >= cohort_start_month,
            first_dates.cohort_month <= cutoff_month,
        ])
        .select("username", "cohort_month")
    )

    # Rename to disambiguate after left join: active_username is NULL for non-active rows,
    # which makes COUNT(DISTINCT active_username) naturally give 0 for those cells.
    activity = (
        active_events
        .select("username", active_month=active_events.dt.truncate("M"))
        .distinct()
        .rename(active_username="username")
    )

    sizes = cohort.group_by("cohort_month").agg(cohort_size=ibis._.count())

    # Rectangular (cohort × month offset) grid, clipped to elapsed months.
    # ibis.range(0, 13).unnest() compiles to LATERAL FLATTEN(ARRAY_GENERATE_RANGE(0, 13))
    # on Snowflake, producing one row per offset 0–12 per cohort month.
    today = ibis.now().date()
    offsets = (
        sizes
        .mutate(month_offset=ibis.range(0, 13))
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
    month_diff = (
        (activity.active_month.year() - expanded.cohort_month.year()) * 12
        + (activity.active_month.month() - expanded.cohort_month.month())
    )

    return (
        expanded
        .left_join(
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
        .rename(str.upper)
    )


def build_command_mau(
    any_command_run: ir.Table, unique_users: ir.Table, keep_prefixes: list[str]
) -> ir.Table:
    """Count monthly unique users per command, filtered to the given two-word prefixes."""
    words = any_command_run.command.split(" ")
    base = (
        any_command_run
        .join(unique_users, "username")[any_command_run.columns]
        .filter(any_command_run.time >= ibis.timestamp("2024-10-01"))
        .mutate(first_two_words=words[0].concat(" ").concat(words[1]))
    )
    return (
        base
        .filter(base.first_two_words.isin(keep_prefixes))
        .mutate(year_month=base.time.truncate("M").strftime("%Y-%m"))
        .group_by(["year_month", "first_two_words"])
        .agg(user_count=base.username.nunique())
        .order_by(["year_month", ibis.desc("user_count")])
        .rename(str.upper)
    )
