import os 

from dagster import Definitions, load_assets_from_modules, EnvVar

from .assets import activities, dbt, energy_prediction, asset_checks
from .resources import database_resource, dbt_resource, strava_api_resouce
from .jobs import activities_update_job
from .schedules import activities_update_schedule

DAGSTER_ENVIRONMENT = EnvVar('DAGSTER_ENVIRONMENT').get_value()

# load in assets from assets/
activities_assets = load_assets_from_modules([activities])
ml_assets = load_assets_from_modules([energy_prediction], group_name="ml_pipeline")
analytics_dbt_assets = load_assets_from_modules([dbt], group_name="dbt_duckdb")

# load in jobs from jobs/
all_jobs = [activities_update_job]
all_schedules = [activities_update_schedule]

defs = Definitions(
    assets=activities_assets + ml_assets + analytics_dbt_assets + ml_assets,
    asset_checks=asset_checks, # defined in assets/__init__.py
    resources={
        "database": database_resource[DAGSTER_ENVIRONMENT],
        "dbt": dbt_resource,
        "strava": strava_api_resouce,
    },
    jobs=all_jobs,
    schedules=all_schedules,
)

