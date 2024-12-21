from dagster import (
    asset,
    EnvVar,
    get_dagster_logger,
)
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
import pendulum

from ..resources import StravaAPIResource, strava_api_resouce

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


@asset(key=["strava", "activities"], group_name="dltHub")
def load_strava_activities():
    """
    dlt EL pipeline based off declarative Rest API Config
    to load raw Strava activities into DuckDB
    """
    duckdb_database_path = EnvVar("DUCKDB_DATABASE").get_value()

    logger.info(f"Dagster Env: {EnvVar('DAGSTER_ENVIRONMENT').get_value()}")
    logger.info(f"Writing to {duckdb_database_path}..")

    pipeline = dlt.pipeline(
        pipeline_name="strava_rest_config",
        destination=dlt.destinations.duckdb(duckdb_database_path),
        dataset_name="activities",
        progress="log",
    )

    source = strava_rest_api_config(strava_api_resouce)

    load_info = pipeline.run(source)
    logger.info(load_info)
