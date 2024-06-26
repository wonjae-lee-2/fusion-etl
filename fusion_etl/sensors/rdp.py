import json
from datetime import datetime
from pathlib import Path

from dagster import (
    EnvVar,
    RunRequest,
    SensorEvaluationContext,
    sensor,
)

from ..jobs import rdp_job
from ..resources.azsql import AzSQLResource

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


def _get_rdp_timestamp(rdp_resource: AzSQLResource) -> str:
    sql = """
        SELECT MAX(ExecutionEndTime)
        FROM srv._ExecutionLog
        WHERE
            PipelineDescription = 'BOARD to DIL'
            AND ExecutionStatus = 'Completed Successfully';
    """

    with rdp_resource.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchall()

    current_rdp_timestamp: datetime = row[0][0]

    return current_rdp_timestamp.strftime("%Y-%m-%d %H:%M:%S")


@sensor(job=rdp_job, minimum_interval_seconds=60 * 60)
def rdp_timestamp_sensor(
    context: SensorEvaluationContext,
    rdp_resource: AzSQLResource,
) -> RunRequest:
    timestamps = _read_timestamps()
    previous_rdp_timestamp = timestamps.get("rdp")
    current_rdp_timestamp = _get_rdp_timestamp(rdp_resource)

    if previous_rdp_timestamp != current_rdp_timestamp:
        timestamps["rdp"] = current_rdp_timestamp

        with open(timestamps_path, "w") as f:
            json.dump(timestamps, f)

    return RunRequest(run_key=current_rdp_timestamp)
