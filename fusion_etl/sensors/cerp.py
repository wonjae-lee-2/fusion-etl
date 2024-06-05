import json
import re
from datetime import datetime
from pathlib import Path

import requests
from dagster import (
    EnvVar,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from playwright.sync_api import Cookie

from ..assets.cerp_mappings import CERP_MAPPINGS
from ..jobs import cerp_active_job, cerp_all_job
from ..resources.bip import BIPublisherResource

timestamps_path = (
    Path(EnvVar("SQLITE_STORAGE_BASE_DIR").get_value())
    .joinpath("timestamps.json")
    .resolve()
)
job_info_url = EnvVar("JOB_INFO_URL").get_value()
output_info_url = EnvVar("OUTPUT_INFO_URL").get_value()
active_cerp_mappings = [x for x in CERP_MAPPINGS if x["status"] == "active"]


def _read_timestamps() -> dict:
    if timestamps_path.is_file():
        with open(timestamps_path, "r") as f:
            timestamps: dict = json.load(f)
    else:
        timestamps = {}

    return timestamps


def _correct_response_text(response_text: str) -> str:
    whitespace_trimmed = response_text.strip()
    keys_quoted = re.sub(r"([{,])(\s*)(\w+)(\s*):", r'\1"\3":', whitespace_trimmed)
    characters_escaped = keys_quoted.replace("\n", "\\n").replace("\r", "\\r")
    single_quotes_replaced = characters_escaped.replace("'", '"')
    return single_quotes_replaced


def _format_job_datetime(job_datetime: int) -> str:
    job_datetime_s = job_datetime / 1000.0
    job_datetime_dt = datetime.fromtimestamp(job_datetime_s)
    formatted_job_datetime = job_datetime_dt.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_job_datetime


def _get_cerp_timestamp(
    cerp_cookies: list[Cookie],
    cerp_mappings: list[dict[str, str]],
) -> dict[str, dict[str, str]]:
    output_timestamps = {}

    for cerp_mapping in cerp_mappings:
        session = requests.Session()
        for cookie in cerp_cookies:
            session.cookies.set(cookie["name"], cookie["value"])
        job_response = session.get(job_info_url.format(cerp_mapping["source"]))
        job_dict = json.loads(_correct_response_text(job_response.text))
        job_datetime = job_dict[0]["dateStarted"]
        formatted_job_datetime = _format_job_datetime(job_datetime)
        job_id = job_dict[0]["id"]
        output_response = session.get(output_info_url.format(job_id))
        output_dict = json.loads(_correct_response_text(output_response.text))
        output_id = output_dict["outputList"][0]["OutputId"]
        output_timestamps[cerp_mapping["source"]] = {
            "output_id": output_id,
            "job_timestamp": formatted_job_datetime,
            "status": cerp_mapping["status"],
        }

    return output_timestamps


def _check_output_ids(
    previous_cerp_timestamp: dict[str, dict[str, str]],
    current_cerp_timestamp: dict[str, dict[str, str]],
) -> bool:
    if not previous_cerp_timestamp:
        return True

    previous_output_ids = [x["output_id"] for x in previous_cerp_timestamp.values()]
    current_active_output_ids = [
        x["output_id"]
        for x in current_cerp_timestamp.values()
        if x["status"] == "active"
    ]
    for output_id in current_active_output_ids:
        if output_id in previous_output_ids:
            return False

    return True


def _get_last_output_id(cerp_timestamp: dict[str, dict[str, str]]) -> str:
    output_ids = [x["output_id"] for x in cerp_timestamp.values()]
    last_output_id = max(output_ids)

    return last_output_id


@sensor(job=cerp_active_job, minimum_interval_seconds=60 * 60)
def cerp_active_timestamp_sensor(
    context: SensorEvaluationContext, cerp_resource: BIPublisherResource
) -> RunRequest | SkipReason:
    timestamps = _read_timestamps()
    previous_cerp_timestamp = timestamps.get("cerp")
    cerp_cookies = cerp_resource.refresh_cookies()
    current_cerp_timestamp = _get_cerp_timestamp(cerp_cookies, CERP_MAPPINGS)
    all_output_ids_changed = _check_output_ids(
        previous_cerp_timestamp,
        current_cerp_timestamp,
    )

    if all_output_ids_changed:
        timestamps["cerp"] = current_cerp_timestamp

        with open(timestamps_path, "w") as f:
            json.dump(timestamps, f)

        last_output_id = _get_last_output_id(current_cerp_timestamp)

        return RunRequest(run_key=last_output_id)
    else:
        return SkipReason(
            "Some or all jobs have not yet run again, so their output IDs have not changed"
        )


@sensor(job=cerp_all_job)
def cerp_all_timestamp_sensor(
    context: SensorEvaluationContext, cerp_resource: BIPublisherResource
) -> RunRequest | SkipReason:
    cerp_cookies = cerp_resource.refresh_cookies()
    current_cerp_timestamp = _get_cerp_timestamp(cerp_cookies, CERP_MAPPINGS)
    timestamps = _read_timestamps()
    timestamps["cerp"] = current_cerp_timestamp

    with open(timestamps_path, "w") as f:
        json.dump(timestamps, f)

    last_output_id = _get_last_output_id(current_cerp_timestamp)

    return RunRequest(run_key=last_output_id)
