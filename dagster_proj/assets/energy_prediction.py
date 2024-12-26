from dagster import (
    asset,
    asset_check,
    AssetCheckResult,
    multi_asset,
    AssetOut,
    MaterializeResult,
    MetadataValue,
    get_dagster_logger,
    AssetExecutionContext
)

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

logger = get_dagster_logger()


@asset(
    deps=["fct_activities", "dim_activities"],
    compute_kind="DuckDB",
    required_resource_keys={"duckdb"},
)
def cycling_data(context: AssetExecutionContext): # duckdb matches the string, 'duckdb'defines in Definitions resource
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
    with context.resources.duckdb.get_connection() as conn:
        df = conn.execute(query).fetch_df()
    logger.info(f"Size of data: {df.shape}")
    return df

@asset_check(asset=cycling_data)
def check_cycling_data_size(cycling_data):
    """Check that the cycling data has more than 1500 rows."""
    num_rows = cycling_data.shape[0]
    passed = num_rows > 1500
    return AssetCheckResult(
        passed=passed,
        metadata={"num_rows": num_rows}
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


@asset
def evaluate_model(trained_model, test_data):
    """Predict and evaluate on testing data"""
    X_test, y_test = test_data
    y_pred = trained_model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    logger.info(f"Mean Squared Error: {mse}")
    logger.info(f"R-squared Score: {r2}")

    return MaterializeResult(
        metadata={"mse": MetadataValue.float(mse), "r2": MetadataValue.float(r2)}
    )
