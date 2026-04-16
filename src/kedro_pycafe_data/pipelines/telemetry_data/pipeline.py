from kedro.pipeline import Pipeline, node

from .nodes import (
    aggregate_project_stats,
    build_commands_mau,
    build_mau,
    build_new_users_monthly,
    build_plugins_mau,
    get_active_events,
    get_unique_users,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                aggregate_project_stats,
                inputs="heap_project_statistics",
                outputs="dt_username",
                name="aggregate_project_stats",
            ),
            node(
                get_unique_users,
                inputs="dt_username",
                outputs="unique_users",
                name="get_unique_users",
            ),
            node(
                get_active_events,
                inputs=["dt_username", "unique_users"],
                outputs="active_events",
                name="get_active_events",
            ),
            node(
                build_new_users_monthly,
                inputs="active_events",
                outputs="new_kedro_users_monthly",
                name="build_new_users_monthly",
            ),
            node(
                build_mau,
                inputs="active_events",
                outputs="mau_kedro",
                name="build_mau",
            ),
            node(
                build_plugins_mau,
                inputs=["heap_any_command_run", "unique_users", "params:plugins"],
                outputs="kedro_plugins_mau",
                name="build_plugins_mau",
            ),
            node(
                build_commands_mau,
                inputs=["heap_any_command_run", "unique_users", "params:commands"],
                outputs="kedro_commands_mau",
                name="build_commands_mau",
            ),
        ]
    )
