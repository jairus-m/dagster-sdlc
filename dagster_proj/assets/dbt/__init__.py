from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from dagster_proj.project import dbt_project


@dbt_assets(
    manifest=dbt_project.manifest_path, dagster_dbt_translator=DagsterDbtTranslator()
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
