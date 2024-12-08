from dagster import ScheduleDefinition
from ..jobs import activities_update_job

activities_update_schedule = ScheduleDefinition(
    job=activities_update_job,
    cron_schedule="0 0 * * *", # every day, at midnight
)