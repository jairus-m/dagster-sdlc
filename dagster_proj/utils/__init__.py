from dagster import get_dagster_logger, AssetExecutionContext
import dlt
from dlt import Pipeline

import pandas as pd

logger = get_dagster_logger()


def dynamic_write_dlt(
    dagster_environment: str,
    duckdb_database_path: str,
    pipeline_name: str,
    dataset_name: str,
) -> Pipeline:
    """
    Based on the env var, DAGSTER_ENVIRONMENT, will create a
    dlt Pipeline to write to either DuckDB (dev) or Snowflake (prod/branch)
    Args:
        dagster_environment (str): DAGSTER_ENVIRONMENT env var (prod, branch, dev)
        duckdb_database_path (str): DUCKDB_DATABASE_PATH env var
        pipeline_name (str): Name of dlt pipeline (for pipeline state/metadata)
        dataset_name: (str): Name of dlt dataset (becomes schema in the target DWH)
    Returns:
        dlt.Pipeline
    """
    if dagster_environment in ("prod", "branch"):
        logger.info("Writing to Snowflake..")
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination="snowflake",
            dataset_name=dataset_name,
            progress="log",
        )
    else:
        logger.info(f"Writing to {duckdb_database_path}..")
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=dlt.destinations.duckdb(duckdb_database_path),
            dataset_name=dataset_name,
            progress="log",
        )

    return pipeline


def dynamic_query(
    dagster_environment: str,
    context: AssetExecutionContext,
    query: str,
) -> pd.DataFrame:
    """
    Based on the env var, DAGSTER_ENVIRONMENT, will query table
    from either DuckDB (dev) or Snowflake (prod/branch).
    Args:
        dagster_environment (str): DAGSTER_ENVIRONMENT env var (prod, branch, dev)
        contect (AssetExecutionContext): context passed in Dagster to get database resource
        query (str): SQL query
    Returns:
        pd.DataFrame
    """
    if dagster_environment == "dev":
        with context.resources.database.get_connection() as conn:
            df = conn.execute(query).fetch_df()
    else:
        with context.resources.database.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                df = cursor.fetch_pandas_all()

                df.columns = df.columns.str.lower()  # make sure col names are lowercase

    return df
