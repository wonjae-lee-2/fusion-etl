import json
from pathlib import Path

from dagster import (
    DagsterRunStatus,
    EnvVar,
    RunRequest,
    RunStatusSensorContext,
    SensorEvaluationContext,
    run_status_sensor,
    sensor,
)

from ..jobs import dbt_job, der_job
from ..resources.der import DERResource

timestamps_path = (
    Path(EnvVar("SQLITE_STORAGE_BASE_DIR").get_value())
    .joinpath("timestamps.json")
    .resolve()
)


def _read_timestamps() -> dict:
    if timestamps_path.is_file():
        with open(timestamps_path, "r") as f:
            timestamps: dict = json.load(f)
    else:
        timestamps = {}

    return timestamps


@sensor(job=der_job, minimum_interval_seconds=60 * 60)
def der_timestamp_sensor(
    context: SensorEvaluationContext, der_resource: DERResource
) -> RunRequest:
    timestamps = _read_timestamps()
    previous_der_timestamp = timestamps.get("der")
    current_der_timestamp = der_resource.get_last_timestamp()

    if previous_der_timestamp != current_der_timestamp:
        timestamps["der"] = current_der_timestamp

        with open(timestamps_path, "w") as f:
            json.dump(timestamps, f)

    return RunRequest(run_key=current_der_timestamp)


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[der_job],
    request_job=dbt_job,
)
def der_run_status_sensor(context: RunStatusSensorContext) -> RunRequest:
    timestamps = _read_timestamps()
    der_timestamp = timestamps.get("der")

    return RunRequest(run_key=der_timestamp)
