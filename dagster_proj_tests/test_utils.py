from dotenv import load_dotenv

from unittest.mock import MagicMock, patch
import pandas as pd

from dagster_proj.utils import dynamic_query, dynamic_write_dlt

load_dotenv()  # load env vars


def test_dynamic_write_dlt_prod():
    """
    Test the dynamic_write_dlt util function for the prod environment.

    This test checks that the function correctly configures a dltHub pipeline
    with Snowflake as the destination for a prod environment.
    """
    with patch("dlt.pipeline") as mock_pipeline:
        dynamic_write_dlt(
            dagster_environment="prod",
            duckdb_database_path="path/to/duckdb",
            pipeline_name="test_pipeline",
            dataset_name="test_dataset",
        )
        mock_pipeline.assert_called_once_with(
            pipeline_name="test_pipeline",
            destination="snowflake",
            dataset_name="test_dataset",
            progress="log",
        )


def test_dynamic_write_dlt_dev():
    """
    Test the dynamic_write_dlt util function in the dev environment.

    This test checks that the function configures a dltHub pipeline
    with DuckDB as the destination for a dev environment.
    """
    with (
        patch("dlt.pipeline") as mock_pipeline,
        patch("dlt.destinations.duckdb") as mock_duckdb,
    ):
        mock_duckdb.return_value = "mock_duckdb_destination"

        dynamic_write_dlt(
            dagster_environment="dev",
            duckdb_database_path="path/to/duckdb",
            pipeline_name="test_pipeline",
            dataset_name="test_dataset",
        )

        mock_pipeline.assert_called_once_with(
            pipeline_name="test_pipeline",
            destination="mock_duckdb_destination",
            dataset_name="test_dataset",
            progress="log",
        )


def test_dynamic_query_prod():
    """
    Test the dynamic_query util function in a prod environment.

    This test ensures that the function properly executes a query and returns
    a pandas DataFrame in the prod environment using a mock db cursor.
    """
    mock_cursor = MagicMock()
    mock_cursor.fetch_pandas_all.return_value = pd.DataFrame({"COL1": [1, 2]})

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    context = MagicMock()
    context.resources.database.get_connection.return_value.__enter__.return_value = (
        mock_conn
    )

    df = dynamic_query(
        dagster_environment="prod", context=context, query="SELECT * FROM table"
    )

    assert df.equals(pd.DataFrame({"col1": [1, 2]}))
    mock_cursor.execute.assert_called_once_with("SELECT * FROM table")


def test_dynamic_query_dev():
    """
    Test the dynamic_query util function in dev environment.

    This test checks if the function correctly executes a query and returns
    a pandas DataFrame in the development environment using a mock db connection.
    """
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetch_df.return_value = pd.DataFrame(
        {"col1": [1, 2]}
    )

    context = MagicMock()
    context.resources.database.get_connection.return_value.__enter__.return_value = (
        mock_conn
    )

    df = dynamic_query(
        dagster_environment="dev", context=context, query="SELECT * FROM table"
    )

    assert df.equals(pd.DataFrame({"col1": [1, 2]}))
    mock_conn.execute.assert_called_once_with("SELECT * FROM table")
