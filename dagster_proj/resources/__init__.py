from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_snowflake import SnowflakeResource
from dagster_dbt import DbtCliResource
from ..project import dbt_project

from .configured_resources import StravaAPIResource

PROFILES_DIR = "analytics_dbt"

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
    profiles_dir=PROFILES_DIR,
    target=EnvVar("DBT_TARGET"),
)

duckdb_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
)

strava_api_resouce = StravaAPIResource(
    client_id=EnvVar("CLIENT_ID").get_value(),
    client_secret=EnvVar("CLIENT_SECRET").get_value(),
    refresh_token=EnvVar("REFRESH_TOKEN").get_value(),
)

snowflake_instance = SnowflakeResource(
    account=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__HOST"),
    user=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME"),
    password=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD"),
    role=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE"),
    database=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE"),
    warehouse=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
)

# will use Dagster env to choose
database_resource = {
    "dev": duckdb_resource,
    "branch": snowflake_instance,
    "prod": snowflake_instance,
}
