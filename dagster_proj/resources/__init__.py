from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource
from ..project import dbt_project

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
    profiles_dir='analytics_dbt',
)

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
)