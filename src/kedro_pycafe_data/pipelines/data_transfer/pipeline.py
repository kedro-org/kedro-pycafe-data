from kedro.pipeline import Node, Pipeline

from .nodes import identity


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            Node(
                identity,
                inputs="pypi_kedro_downloads",
                outputs="pypi_kedro_downloads_table",
                name="fetch_and_save_snowflake_data",
            ),
            Node(
                identity,
                inputs="downloads_by_country",
                outputs="downloads_by_country_table",
                name="fetch_and_save_downloads_by_country",
            ),
            Node(
                func=fetch_and_save,
                inputs="pypi_plugin_downloads",
                outputs="pypi_plugin_downloads_table",
                name="fetch_and_save_plugin_downloads",
            ),
        ]
    )
