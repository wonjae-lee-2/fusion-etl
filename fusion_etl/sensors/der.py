import json
from pathlib import Path

from dagster import (
    EnvVar,
    RunRequest,
    SensorEvaluationContext,
    sensor,
)

from ..jobs import der_job
from ..resources.pbi import PowerBIResource

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


def _get_last_timestamp(der_resource: PowerBIResource) -> str:
    dax = """
        EVALUATE
        ROW ( "Timestamp", MAX ( FactIncomeWIP[DataRefreshDateTime] ) )
    """

    timestamp = der_resource.execute(dax)
    return timestamp[0]["[Timestamp]"]


@sensor(job=der_job, minimum_interval_seconds=60 * 60)
def der_timestamp_sensor(
    context: SensorEvaluationContext, der_resource: PowerBIResource
) -> RunRequest:
    timestamps = _read_timestamps()
    previous_der_timestamp = timestamps.get("der")
    current_der_timestamp = _get_last_timestamp(der_resource)

    if previous_der_timestamp != current_der_timestamp:
        timestamps["der"] = current_der_timestamp

        with open(timestamps_path, "w") as f:
            json.dump(timestamps, f)

    return RunRequest(run_key=current_der_timestamp)
