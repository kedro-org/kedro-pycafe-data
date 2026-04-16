import ibis
import ibis.expr.types as ir


def get_unique_users(heap_stats: ir.Table) -> ir.Table:
    """Filter to users active on >8 distinct days since 2024-09-01."""
    base = (
        heap_stats
        .filter([
            heap_stats.time.date() >= ibis.date("2024-09-01"),
            heap_stats.is_ci_env.isnull() | (heap_stats.is_ci_env == "false"),
        ])
        .group_by(["username", heap_stats.time.date().name("dt")])
        .agg(max_version_prefix=heap_stats.project_version.left(4).max())
    )
    min_max = base.group_by("username").agg(
        min_dt=base.dt.min(), max_dt=base.dt.max()
    )
    return (
        min_max
        .filter(min_max.max_dt.delta(min_max.min_dt, "day") > 8)
        .select("username")
    )


def get_active_events(heap_stats: ir.Table, unique_users: ir.Table) -> ir.Table:
    """Join base event aggregates back to the unique-user list."""
    base = (
        heap_stats
        .filter([
            heap_stats.time.date() >= ibis.date("2024-09-01"),
            heap_stats.is_ci_env.isnull() | (heap_stats.is_ci_env == "false"),
        ])
        .group_by(["username", heap_stats.time.date().name("dt")])
        .agg(max_version_prefix=heap_stats.project_version.left(4).max())
    )
    return base.join(unique_users, "username")[base.columns]


def build_new_users_monthly(active_events: ir.Table) -> ir.Table:
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
    )


def build_mau(active_events: ir.Table) -> ir.Table:
    return (
        active_events
        .filter(active_events.dt >= ibis.date("2024-10-01"))
        .mutate(year_month=active_events.dt.truncate("M").strftime("%Y-%m"))
        .group_by(["year_month", "max_version_prefix"])
        .agg(mau=active_events.username.nunique())
        .order_by(["year_month", "max_version_prefix"])
    )


def build_plugins_mau(
    any_command_run: ir.Table, unique_users: ir.Table, plugins: list[str]
) -> ir.Table:
    return _build_command_mau(any_command_run, unique_users, plugins)


def build_commands_mau(
    any_command_run: ir.Table, unique_users: ir.Table, commands: list[str]
) -> ir.Table:
    return _build_command_mau(any_command_run, unique_users, commands)


def _build_command_mau(
    any_command_run: ir.Table, unique_users: ir.Table, keep_prefixes: list[str]
) -> ir.Table:
    words = any_command_run.command.split(" ")
    base = (
        any_command_run
        .join(unique_users, any_command_run.username == unique_users.username)
        .filter(any_command_run.time >= ibis.timestamp("2024-10-01"))
        .mutate(first_two_words=(words[0].concat(" ").concat(words[1])))
    )
    return (
        base
        .filter(base.first_two_words.isin(keep_prefixes))
        .mutate(year_month=base.time.truncate("M").strftime("%Y-%m"))
        .group_by(["year_month", "first_two_words"])
        .agg(unique_users=base.username.nunique())
        .order_by(["year_month", ibis.desc("unique_users")])
    )
