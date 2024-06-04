from dagster import (
    DagsterRunStatus,
    RunRequest,
    RunStatusSensorContext,
    run_status_sensor,
)

from ..jobs import dbt_job, der_job, erp_active_job, erp_all_job, rdp_job


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    minimum_interval_seconds=60 * 60,
    monitored_jobs=[der_job, erp_active_job, erp_all_job, rdp_job],
    request_job=dbt_job,
)
def dbt_run_status_sensor(context: RunStatusSensorContext) -> RunRequest:
    return RunRequest()
