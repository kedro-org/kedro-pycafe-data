from kedro.pipeline import Pipeline, node

from .nodes import identity


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                identity,
                inputs="pypi_kedro_downloads",
                outputs="pypi_kedro_downloads_table",
                name="fetch_and_save_snowflake_data",
            ),
            node(
                identity,
                inputs="downloads_by_country",
                outputs="downloads_by_country_table",
                name="fetch_and_save_downloads_by_country",
            ),
        ]
    )
