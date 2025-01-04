from dagster import Definitions, load_assets_from_modules, EnvVar

from .assets import dlt, dbt, ml_analytics
from .resources import database_resource, dbt_resource, strava_api_resource
from .jobs import activities_update_job
from .schedules import activities_update_schedule

DAGSTER_ENVIRONMENT = EnvVar("DAGSTER_ENVIRONMENT").get_value()

# load in assets from assets/
activities_assets = load_assets_from_modules([dlt.activities])
ml_assets = load_assets_from_modules(
    [ml_analytics.energy_prediction], group_name="ml_pipeline"
)
dashboard_assets = load_assets_from_modules(
    [ml_analytics.weekly_totals], group_name="analytics"
)
analytics_dbt_assets = load_assets_from_modules([dbt], group_name="dbt_duckdb")

# load in jobs from jobs/
all_jobs = [activities_update_job]
all_schedules = [activities_update_schedule]

defs = Definitions(
    assets=activities_assets + analytics_dbt_assets + ml_assets + dashboard_assets,
    asset_checks=ml_analytics.asset_checks,  # defined in assets/ml_analytics/__init__.py
    resources={
        "database": database_resource[DAGSTER_ENVIRONMENT],
        "dbt": dbt_resource,
        "strava": strava_api_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
)
