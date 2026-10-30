from kedro.pipeline import Node, Pipeline

from .nodes import export_downloads, identity


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            Node(
                export_downloads,
                inputs=["pypi_kedro_downloads", "pypi_kedro_downloads_extended"],
                outputs=[
                    "pypi_kedro_downloads_table",
                    "pypi_kedro_downloads_split_table",
                ],
                name="fetch_and_save_snowflake_data",
            ),
            Node(
                identity,
                inputs="downloads_by_country",
                outputs="downloads_by_country_table",
                name="fetch_and_save_downloads_by_country",
            ),
            Node(
                identity,
                inputs="pypi_plugin_downloads",
                outputs="pypi_plugin_downloads_table",
                name="fetch_and_save_plugin_downloads",
            ),
        ]
    )
