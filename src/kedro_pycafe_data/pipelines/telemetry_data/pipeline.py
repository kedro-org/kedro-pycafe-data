from kedro.pipeline import Pipeline, node

from .nodes import (
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
                get_unique_users,
                inputs="heap_project_statistics",
                outputs="unique_users",
            ),
            node(
                get_active_events,
                inputs=["heap_project_statistics", "unique_users"],
                outputs="active_events",
            ),
            node(
                build_new_users_monthly,
                inputs="active_events",
                outputs="new_kedro_users_monthly",
            ),
            node(
                build_mau,
                inputs="active_events",
                outputs="mau_kedro",
            ),
            node(
                build_plugins_mau,
                inputs=["heap_any_command_run", "unique_users", "params:plugins"],
                outputs="kedro_plugins_mau",
            ),
            node(
                build_commands_mau,
                inputs=["heap_any_command_run", "unique_users", "params:commands"],
                outputs="kedro_commands_mau",
            ),
        ]
    )
