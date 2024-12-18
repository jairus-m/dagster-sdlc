from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource
from ..project import dbt_project

from .configured_resources import StravaAPIResource

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
    profiles_dir='analytics_dbt',
)

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
)

strava_api_resouce = StravaAPIResource(
    client_id=EnvVar("CLIENT_ID").get_value(),
    client_secret=EnvVar("CLIENT_SECRET").get_value(),
    refresh_token=EnvVar("REFRESH_TOKEN").get_value(),
)
