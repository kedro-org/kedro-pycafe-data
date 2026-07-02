from kedro.pipeline import Node, Pipeline

from .nodes import (
    aggregate_project_stats,
    build_cohort_retention,
    build_command_mau,
    build_experimental_dataset_usage,
    build_mau,
    build_new_users_monthly,
    get_active_events,
    get_unique_users,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            Node(
                aggregate_project_stats,
                inputs="heap_project_statistics",
                outputs="dt_username",
                name="aggregate_project_stats",
            ),
            Node(
                get_unique_users,
                inputs="dt_username",
                outputs="unique_users",
                name="get_unique_users",
            ),
            Node(
                get_active_events,
                inputs=["dt_username", "unique_users"],
                outputs="active_events",
                name="get_active_events",
            ),
            Node(
                build_new_users_monthly,
                inputs="active_events",
                outputs="new_kedro_users_monthly",
                name="build_new_users_monthly",
            ),
            Node(
                build_mau,
                inputs="active_events",
                outputs="mau_kedro",
                name="build_mau",
            ),
            Node(
                build_command_mau,
                inputs=["heap_any_command_run", "unique_users", "params:plugins"],
                outputs="kedro_plugins_mau",
                name="build_plugins_mau",
            ),
            Node(
                build_command_mau,
                inputs=["heap_any_command_run", "unique_users", "params:commands"],
                outputs="kedro_commands_mau",
                name="build_commands_mau",
            ),
            Node(
                build_cohort_retention,
                inputs=["active_events", "params:cohort_trailing_hide_months"],
                outputs="cohort_retention",
                name="build_cohort_retention",
            ),
            Node(
                build_experimental_dataset_usage,
                inputs=["heap_project_statistics", "params:genai_min_users"],
                outputs=[
                    "experimental_dataset_usage_monthly",
                    "experimental_dataset_usage_summary",
                    "experimental_tool_usage_summary",
                ],
                name="build_experimental_dataset_usage",
            ),
        ]
    )
