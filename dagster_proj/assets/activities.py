import pendulum

from dagster import (
    asset,
    EnvVar,
    get_dagster_logger,
    AssetExecutionContext,
    MonthlyPartitionsDefinition,
)
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
import pendulum

from ..resources import StravaAPIResource

logger = get_dagster_logger()


@dlt.source
def strava_rest_api_config(strava_resource: StravaAPIResource, start_date_int: int, end_date_int: int):
    """
    Strava Activities endpoint using the dltHub RESTAPIConfig class and rest_api_resources function.
    Partitioned with start_date and end_date (MonthlyPartitionsDefinition).
    Args:
        strava_resource (StravaAPIResource): the Dagster resource object passed in via the Definitions object
        start_date (int): epoch/unix time to query "after" in activities
        end_date (int): epoch/unix time to query "before" in activities
    Yields:
         List[DltResource]
    """
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
                        "after": start_date_int,
                        "before": end_date_int,
                        "per_page": 30,
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
    partitions_def=MonthlyPartitionsDefinition(start_date="2010-01-01"),
)
def load_strava_activities(context: AssetExecutionContext):
    """
    dlt EL pipeline based off declarative Rest API Config
    to load raw Strava activities into DuckDB
    """
    duckdb_database_path = EnvVar("DUCKDB_DATABASE").get_value()

    logger.info(f"Dagster Env: {EnvVar('DAGSTER_ENVIRONMENT').get_value()}")
    logger.info(f"Writing to {duckdb_database_path}..")

    # Get the start and end dates for the current partition
    start_date = context.partition_key
    end_date = pendulum.parse(start_date).add(months=1).to_date_string()
    start_date_int = int(pendulum.parse(start_date).timestamp())
    end_date_int = int(pendulum.parse(end_date).timestamp())

    logger.info(f'Partition start: {start_date} ({start_date_int})') # just want to make sure the partitions are correct
    logger.info(f'Partition end: {end_date} ({end_date_int})')

    pipeline = dlt.pipeline(
        pipeline_name="strava_rest_config",
        destination=dlt.destinations.duckdb(duckdb_database_path),
        dataset_name="activities",
        progress="log",
    )

    source = strava_rest_api_config(
        context.resources.strava,
        start_date_int,
        end_date_int,
    )

    load_info = pipeline.run(source)
    logger.info(load_info)
