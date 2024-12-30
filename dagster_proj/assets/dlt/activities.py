from dagster import (
    asset,
    EnvVar,
    get_dagster_logger,
    AssetExecutionContext,
)

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

import pendulum

from ...resources import StravaAPIResource
from ...utils import dynamic_write_dlt

logger = get_dagster_logger()


@dlt.source
def strava_rest_api_config(strava_resource: StravaAPIResource):
    logger.info("Extracting Strava data source")
    access_token = strava_resource.get_access_token()

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.strava.com/api/v3/",
            "auth": {
                "type": "bearer",
                "token": access_token,
            },
            "paginator": {"type": "page_number", "base_page": 1, "total_path": None},
        },
        "resources": [
            {
                "name": "activities",
                "endpoint": {
                    "params": {
                        "after": {
                            "type": "incremental",
                            "cursor_path": "start_date_local",
                            "initial_value": "2010-01-01 00:00:00+00:00",
                            "convert": lambda dt_str: int(
                                pendulum.parse(dt_str).timestamp()
                            ),
                        },
                    }
                },
                "primary_key": "id",
                "write_disposition": "merge",
            }
        ],
    }

    logger.info("RESTAPIConfig set up, starting to yield resources...")

    yield from rest_api_resources(config)


@asset(
    key=["strava", "activities"],
    group_name="dltHub",
    required_resource_keys={"strava"},
)
def load_strava_activities(context: AssetExecutionContext):
    """
    dlt EL pipeline based off declarative Rest API Config
    to load raw Strava activities into DuckDB
    """
    DUCKDB_DATABASE_PATH = EnvVar("DUCKDB_DATABASE").get_value()
    DAGSTER_ENVIRONMENT = EnvVar("DAGSTER_ENVIRONMENT").get_value()

    logger.info(f"Dagster Env: {DAGSTER_ENVIRONMENT}")

    # util function to write to DuckDB or Snowflake based on environment
    pipeline = dynamic_write_dlt(
        dagster_environment=DAGSTER_ENVIRONMENT,
        duckdb_database_path=DUCKDB_DATABASE_PATH,
        pipeline_name="strava_rest_api_config",
        dataset_name="strava_data" # schema in dwh
    )

    source = strava_rest_api_config(context.resources.strava)
    load_info = pipeline.run(source)
    logger.info(load_info)
