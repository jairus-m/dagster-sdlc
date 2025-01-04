from dagster import (
    multi_asset,
    EnvVar,
    AssetOut,
    Output,
    get_dagster_logger,
    AssetExecutionContext,
)

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator

import pendulum

from dagster_proj.resources import StravaAPIResource
from dagster_proj.utils import dynamic_write_dlt

logger = get_dagster_logger()


def strava_activities():
    """
    dltHub RESTAPIConfig "resources" config for activities endpoint.
    Args:
        None
    Returns:
        dict[str, Any]
    """
    return {
        "name": "activities",
        "endpoint": {
            "params": {
                "after": {
                    "type": "incremental",
                    "cursor_path": "start_date_local",
                    "initial_value": "2010-01-01 00:00:00+00:00",
                    "convert": lambda dt_str: int(pendulum.parse(dt_str).timestamp()),
                },
            }
        },
        "primary_key": "id",
        "write_disposition": "merge",
    }


def strava_stats(athlete_id: int):
    """
    dltHub RESTAPIConfig "resources" config for stats endpoint.
    Args:
        athlete_id (int): athlete id
    Returns:
        dict[str, Any]
    """
    return {
        "name": "stats",
        "endpoint": {
            "path": f"athletes/{athlete_id}/stats",
            "paginator": SinglePagePaginator(),
        },
        "write_disposition": "replace",
    }


@dlt.source
def strava_rest_api_config(strava_resource: StravaAPIResource):
    logger.info("Extracting Strava data source")
    access_token = strava_resource.get_access_token()
    athlete_id = strava_resource.athlete_id

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.strava.com/api/v3/",
            "auth": {
                "type": "bearer",
                "token": access_token,
            },
            "paginator": {"type": "page_number", "base_page": 1, "total_path": None},
        },
        "resources": [strava_activities(), strava_stats(athlete_id)],
    }

    logger.info("RESTAPIConfig set up, starting to yield resources...")

    yield from rest_api_resources(config)


@multi_asset(
    outs={
        "activities": AssetOut(
            key=["strava", "activities"], description="Raw activity data from Strava."
        ),
        "stats": AssetOut(
            key=["strava", "stats"], description="Summary athlete stats."
        ),
    },
    group_name="dltHub",
    required_resource_keys={"strava"},
)
def load_strava_activities(context: AssetExecutionContext):
    """
    dlt EL pipeline based off declarative Rest API Config
    to load raw Strava activities/athlete stats
    """
    DUCKDB_DATABASE_PATH = EnvVar("DUCKDB_DATABASE").get_value()
    DAGSTER_ENVIRONMENT = EnvVar("DAGSTER_ENVIRONMENT").get_value()

    logger.info(f"Dagster Env: {DAGSTER_ENVIRONMENT}")

    # util function to write to DuckDB or Snowflake based on environment
    pipeline = dynamic_write_dlt(
        dagster_environment=DAGSTER_ENVIRONMENT,
        duckdb_database_path=DUCKDB_DATABASE_PATH,
        pipeline_name="strava_rest_api_config",
        dataset_name="strava_data",  # schema in dwh
    )

    source = strava_rest_api_config(context.resources.strava)
    load_info = pipeline.run(source)
    logger.info(load_info)

    yield Output(value=load_info[0], output_name="activities")
    yield Output(value=load_info[1], output_name="stats")
