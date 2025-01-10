from collections.abc import Iterable

from dagster import (
    EnvVar,
    AssetKey,
    get_dagster_logger,
    AssetExecutionContext,
)
from dagster_embedded_elt.dlt import (
    DagsterDltTranslator,
    dlt_assets,
    DagsterDltResource,
)

from dlt.sources.rest_api import rest_api_source
from dlt.extract.resource import DltResource
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator

import pendulum

from dagster_proj.resources import strava_api_resource
from dagster_proj.utils import dynamic_write_dlt

logger = get_dagster_logger()

DUCKDB_DATABASE_PATH = EnvVar("DUCKDB_DATABASE").get_value()
DAGSTER_ENVIRONMENT = EnvVar("DAGSTER_ENVIRONMENT").get_value()

ACCESS_TOKEN = strava_api_resource.get_access_token()
ATHLETE_ID = strava_api_resource.athlete_id


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


strava_source = rest_api_source(
    {
        "client": {
            "base_url": "https://www.strava.com/api/v3/",
            "auth": {
                "type": "bearer",
                "token": ACCESS_TOKEN,
            },
            "paginator": {"type": "page_number", "base_page": 1, "total_path": None},
        },
        "resources": [strava_activities(), strava_stats(ATHLETE_ID)],
    }
)


class CustomDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_key(self, resource: DltResource) -> AssetKey:
        """Overrides asset key to be the dlt resource name with a 'strava' prefix."""
        return AssetKey(f"{resource.name}").with_prefix("strava")

    def get_deps_asset_keys(self, resource: DltResource) -> Iterable[AssetKey]:
        """Overrides upstream asset key to be a single source asset."""
        return []


@dlt_assets(
    dlt_source=strava_source,
    dlt_pipeline=dynamic_write_dlt(
        dagster_environment=DAGSTER_ENVIRONMENT,
        duckdb_database_path=DUCKDB_DATABASE_PATH,
        pipeline_name="strava_rest_api_config",
        dataset_name="strava_data",  # schema in dwh
    ),
    group_name="dltHub",
    dagster_dlt_translator=CustomDagsterDltTranslator(),
)
def load_strava_activities(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
