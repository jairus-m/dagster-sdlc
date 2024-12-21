from dagster import Definitions, load_assets_from_modules

from .assets import activities, dbt, energy_prediction
from .resources import database_resource, dbt_resource, strava_api_resouce
from .jobs import activities_update_job
from .schedules import activities_update_schedule

activities_assets = load_assets_from_modules([activities])
ml_assets = load_assets_from_modules([energy_prediction], group_name="ml_pipeline")
analytics_dbt_assets = load_assets_from_modules([dbt], group_name="dbt_duckdb")

all_jobs = [activities_update_job]
all_schedules = [activities_update_schedule]

defs = Definitions(
    assets=activities_assets + ml_assets + analytics_dbt_assets + ml_assets,
    resources={
        "database": database_resource,
        "dbt": dbt_resource,
        "strava": strava_api_resouce,
    },
    jobs=all_jobs,
    schedules=all_schedules,
)
