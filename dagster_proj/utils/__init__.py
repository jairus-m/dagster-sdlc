from dagster import get_dagster_logger
import dlt
from dlt import Pipeline

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
