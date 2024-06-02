import json
import re
from datetime import datetime
from pathlib import Path

import requests
from dagster import (
    DagsterRunStatus,
    EnvVar,
    RunRequest,
    RunStatusSensorContext,
    SensorEvaluationContext,
    SkipReason,
    run_status_sensor,
    sensor,
)
from playwright.sync_api import Cookie

from ..assets.erp_mappings import ERP_MAPPINGS
from ..jobs import dbt_job, erp_active_job
from ..resources.erp import ERPResource

timestamps_path = (
    Path(EnvVar("SQLITE_STORAGE_BASE_DIR").get_value())
    .joinpath("timestamps.json")
    .resolve()
)
erp_cookies_path = (
    Path(EnvVar("SQLITE_STORAGE_BASE_DIR").get_value())
    .joinpath("erp_cookies.pkl")
    .resolve()
)
job_info_url = EnvVar("JOB_INFO_URL").get_value()
output_info_url = EnvVar("OUTPUT_INFO_URL").get_value()
active_erp_mappings = [x for x in ERP_MAPPINGS if x["status"] == "active"]


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


def _get_erp_timestamp(
    erp_cookies: list[Cookie],
    erp_mappings: list[dict[str, str]],
) -> dict[str, dict[str, str]]:
    output_timestamps = {}

    for erp_mapping in erp_mappings:
        print(f"getting timestamp for {erp_mapping['source']}")
        session = requests.Session()
        for cookie in erp_cookies:
            session.cookies.set(cookie["name"], cookie["value"])
        job_response = session.get(job_info_url.format(erp_mapping["source"]))
        job_dict = json.loads(_correct_response_text(job_response.text))
        job_datetime = job_dict[0]["dateStarted"]
        formatted_job_datetime = _format_job_datetime(job_datetime)
        job_id = job_dict[0]["id"]
        output_response = session.get(output_info_url.format(job_id))
        output_dict = json.loads(_correct_response_text(output_response.text))
        output_id = output_dict["outputList"][0]["OutputId"]
        output_timestamps[erp_mapping["source"]] = {
            "output_id": output_id,
            "job_timestamp": formatted_job_datetime,
            "status": erp_mapping["status"],
        }
        print("... done")

    return output_timestamps


def _check_output_ids(
    previous_erp_timestamp: dict[str, dict[str, str]],
    current_erp_timestamp: dict[str, dict[str, str]],
) -> bool:
    if not previous_erp_timestamp:
        return True

    previous_output_ids = [x["output_id"] for x in previous_erp_timestamp.values()]
    current_active_output_ids = [
        x["output_id"]
        for x in current_erp_timestamp.values()
        if x["status"] == "active"
    ]
    for output_id in current_active_output_ids:
        if output_id in previous_output_ids:
            return False

    return True


def _get_last_output_id(erp_timestamp: dict[str, dict[str, str]]) -> str:
    output_ids = [x["output_id"] for x in erp_timestamp.values()]
    last_output_id = max(output_ids)

    return last_output_id


@sensor(job=erp_active_job, minimum_interval_seconds=60 * 60)
def erp_active_timestamp_sensor(
    context: SensorEvaluationContext, erp_resource: ERPResource
) -> RunRequest | SkipReason:
    timestamps = _read_timestamps()
    previous_erp_timestamp = timestamps.get("erp")
    erp_cookies = erp_resource.refresh_cookies()
    current_erp_timestamp = _get_erp_timestamp(erp_cookies, ERP_MAPPINGS)
    all_output_ids_changed = _check_output_ids(
        previous_erp_timestamp,
        current_erp_timestamp,
    )

    if all_output_ids_changed:
        timestamps["erp"] = current_erp_timestamp

        with open(timestamps_path, "w") as f:
            json.dump(timestamps, f)

        last_output_id = _get_last_output_id(current_erp_timestamp)

        return RunRequest(run_key=last_output_id)
    else:
        return SkipReason(
            "Some or all jobs have not yet run again, so their output IDs have not changed"
        )


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[erp_active_job],
    request_job=dbt_job,
)
def erp_active_run_status_sensor(context: RunStatusSensorContext) -> RunRequest:
    timestamps = _read_timestamps()
    erp_timestamp = timestamps.get("erp")
    last_output_id = _get_last_output_id(erp_timestamp)

    return RunRequest(run_key=last_output_id)
