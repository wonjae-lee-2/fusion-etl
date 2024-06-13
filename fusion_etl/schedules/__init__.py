from dagster import ScheduleDefinition

from ..jobs import orion_job

orion_daily_schedule = ScheduleDefinition(
    name="orion_daily_schedule",
    job=orion_job,
    cron_schedule="0 1 * * *",
)
