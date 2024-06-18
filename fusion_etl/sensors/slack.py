from datetime import datetime

from dagster import (
    DagsterRunStatus,
    RunConfig,
    RunRequest,
    RunStatusSensorContext,
    run_status_sensor,
)

from ..jobs import (
    dbt_job,
    der_job,
    erp_active_job,
    erp_all_job,
    orion_job,
    rdp_job,
    send_slack_message_job,
    sp_job,
)
from ..ops import MessageConfig


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[
        der_job,
        erp_active_job,
        erp_all_job,
        orion_job,
        rdp_job,
        sp_job,
        dbt_job,
    ],
    request_job=send_slack_message_job,
)
def slack_run_success_sensor(context: RunStatusSensorContext) -> RunRequest:
    job_name = context.dagster_run.job_name
    run_id = context.dagster_run.run_id
    run_end_time = context.instance.get_run_stats(run_id).end_time
    timestamp = datetime.fromtimestamp(run_end_time).strftime(
        "%Y-%m-%d %I:%M:%S %p UTC"
    )
    message = f"{job_name} has completed successfully at {timestamp}."
    run_config = RunConfig(ops={"send_slack_message": MessageConfig(message=message)})
    return RunRequest(run_config=run_config)


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    monitored_jobs=[
        der_job,
        erp_active_job,
        erp_all_job,
        orion_job,
        rdp_job,
        sp_job,
        dbt_job,
    ],
    request_job=send_slack_message_job,
)
def slack_run_failure_sensor(context: RunStatusSensorContext) -> RunRequest:
    job_name = context.dagster_run.job_name
    run_id = context.dagster_run.run_id
    end_time = context.instance.get_run_stats(run_id).end_time
    timestamp = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %I:%M:%S %p UTC")
    message = f"{job_name} has failed at {timestamp}."
    run_config = RunConfig(ops={"send_slack_message": MessageConfig(message=message)})
    return RunRequest(run_config=run_config)
