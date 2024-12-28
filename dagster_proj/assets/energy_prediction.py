import os
from pathlib import Path

from dagster import (
    asset,
    asset_check,
    AssetCheckResult,
    multi_asset,
    AssetOut,
    MaterializeResult,
    MetadataValue,
    get_dagster_logger,
    AssetExecutionContext,
    EnvVar,
)

from ..utils import dynamic_query

import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.preprocessing import StandardScaler, FunctionTransformer
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor

import plotly.graph_objects as go

logger = get_dagster_logger()

DAGSTER_ENVIRONMENT = EnvVar("DAGSTER_ENVIRONMENT").get_value()


@asset(
    deps=["fct_activities", "dim_activities"],
    compute_kind="Snowflake",
    required_resource_keys={"database"},
)
def cycling_data(
    context: AssetExecutionContext,
):  # duckdb matches the string, 'duckdb'defines in Definitions resource
    """Gets cycling data from DuckDB"""
    query = """
        select 
            distance_miles as distance
            , moving_time_minutes as moving_time
            , total_elevation_gain_feet as total_elevation_gain
            , average_speed_mph as average_speed
            , average_watts
            , weighted_average_watts
            , kilojoules
            , average_heartrate
        from dbt.fct_activities as fct
        left join dbt.dim_activities as dim
            on fct.id = dim.id
        where sport_type = 'Cycling'
            and average_watts is not null
    """
    df = dynamic_query(
        dagster_environment=DAGSTER_ENVIRONMENT,
        context=context,
        query=query,
    )

    context.log.info(f"Size of data: {df.shape}")

    return df


@asset_check(asset=cycling_data)
def check_cycling_data_size(cycling_data):
    """Check that the cycling data has more than 1500 rows."""
    num_rows = cycling_data.shape[0]
    num_cols = cycling_data.shape[1]
    passed = num_rows > 1500 & num_cols == 8
    return AssetCheckResult(
        passed=passed, metadata={"num_rows": num_rows, "num_cols": num_cols}
    )


@multi_asset(outs={"training_data": AssetOut(), "test_data": AssetOut()})
def preprocess_data(cycling_data):
    """Preprocesses data (train/test split of X/y)"""
    X = cycling_data.loc[:, ~cycling_data.columns.isin(["kilojoules"])]
    y = np.sqrt(
        cycling_data.loc[:, cycling_data.columns.isin(["kilojoules"])]
    ).values.ravel()
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=1998
    )
    return (X_train, y_train), (X_test, y_test)


@asset
def sklearn_preprocessor():
    """Implement Sklearn data preprocessor pipeline"""
    sqrt_pipeline = Pipeline(
        [
            ("impute_regression", IterativeImputer(estimator=LinearRegression())),
            (
                "sqrt_transform",
                FunctionTransformer(np.sqrt, feature_names_out="one-to-one"),
            ),
            ("standard_scaler", StandardScaler()),
        ]
    )

    numeric_pipeline = Pipeline(
        [
            ("impute_regression", IterativeImputer(estimator=LinearRegression())),
            ("standard_scaler", StandardScaler()),
        ]
    )

    return ColumnTransformer(
        [
            (
                "nums",
                numeric_pipeline,
                [
                    "distance",
                    "moving_time",
                    "average_speed",
                    "average_watts",
                    "weighted_average_watts",
                ],
            ),
            ("sqrt_transform", sqrt_pipeline, ["total_elevation_gain"]),
        ],
        remainder="drop",
    )


@asset
def random_forest_pipeline(sklearn_preprocessor):
    """Sklearn preprocessor + random forest pipeline"""
    return Pipeline(
        [
            ("preprocess", sklearn_preprocessor),
            (
                "random_forest",
                RandomForestRegressor(n_estimators=100, random_state=1998),
            ),  # hehe :3
        ]
    )


@asset
def trained_model(training_data, random_forest_pipeline):
    """Train the model"""
    X_train, y_train = training_data
    rf_pipeline = random_forest_pipeline
    rf_pipeline.fit(X_train, y_train)
    return rf_pipeline


@multi_asset(outs={"results_df": AssetOut(), "model_evaluation": AssetOut()})
def evaluate_model(trained_model, test_data):
    """Predict and evaluate on testing data"""
    X_test, y_test = test_data
    y_pred = trained_model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    logger.info(f"Mean Squared Error: {mse}")
    logger.info(f"R-squared Score: {r2}")

    results_df = pd.DataFrame(X_test)
    results_df['Predicted'] = y_pred

    return (
            results_df,
            MaterializeResult(
                asset_key="model_evaluation",
                metadata={"mse": MetadataValue.float(mse), "r2": MetadataValue.float(r2)}
            )
        )

@asset 
def create_scatter_plot(context: AssetExecutionContext, results_df):
    # Create a scatter plot
    fig = go.Figure()

    # Add scatter points for X_test
    fig.add_trace(go.Scatter(
        x=results_df.index,
        y=results_df.iloc[:, 0],
        mode='markers',
        name='X_test',
        marker=dict(size=8)
    ))

    # Add scatter points for y_pred
    fig.add_trace(go.Scatter(
        x=results_df.index,
        y=results_df['Predicted'],
        mode='markers',
        name='Predicted',
        marker=dict(color='red', size=10)
    ))

    # Update layout
    fig.update_layout(
        title='Scatter Plot of X_test and Predicted Values',
        xaxis_title='Index',
        yaxis_title='Values',
        legend=dict(title='Legend'),
        template='plotly_white'
    )

    # Define the path to save the HTML file
    save_chart_path = Path(context.instance.storage_directory()).joinpath("ml_results.html")
    
    # Save the figure as an HTML file
    fig.write_html(save_chart_path, auto_open=True)

    # Add metadata to make the HTML file accessible from the Dagster UI
    context.add_output_metadata({
        "plot_url": MetadataValue.url("file://" + os.fspath(save_chart_path))
    })

    # Show the plot
    return fig
