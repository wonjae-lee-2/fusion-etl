import json
import pickle
from pathlib import Path

import requests
from dagster import AssetsDefinition, EnvVar, MaterializeResult, asset
from playwright.sync_api import Cookie

from ..resources.azblob import AzBlobResource
from ..resources.azsql import AzSQLResource

timestamps_path = (
    Path(EnvVar("SQLITE_STORAGE_BASE_DIR").get_value())
    .joinpath("timestamps.json")
    .resolve()
)
cerp_cookies_path = (
    Path(EnvVar("SQLITE_STORAGE_BASE_DIR").get_value())
    .joinpath("cerp_cookies.pkl")
    .resolve()
)
output_download_url = EnvVar("OUTPUT_DOWNLOAD_URL").get_value()


def _read_cerp_timestamps(timestamps_path: Path) -> dict[str, dict[str, str]]:
    if not timestamps_path.is_file():
        raise FileNotFoundError(
            f"timestamps.json was not found at {timestamps_path}. Please run the cerp timestamp sensor first."
        )

    with open(timestamps_path, "r") as f:
        timestamps: dict = json.load(f)
    cerp_timestamps: dict[str, dict[str, str]] = timestamps.get("cerp")

    if not cerp_timestamps:
        raise ValueError(
            "The cerp timestamp was not found in timestamps.json. Please run the cerp timestamp sensor first."
        )

    return cerp_timestamps


def _read_output_timestamp(
    timestamps_path: Path,
    cerp_mapping: dict[str, str],
) -> tuple[str, str]:
    cerp_timestamps = _read_cerp_timestamps(timestamps_path)

    output_id = cerp_timestamps.get(cerp_mapping["source"]).get("output_id")
    job_timestamp = cerp_timestamps.get(cerp_mapping["source"]).get("job_timestamp")

    return (output_id, job_timestamp)


def define_cerp_blob_asset(
    cerp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"cerp_blob__{cerp_mapping['name']}"

    @asset(
        key_prefix="cerp",
        group_name="cerp_blob",
        name=asset_name,
        compute_kind="azure",
        tags={"status": cerp_mapping["status"]},
    )
    def _cerp_blob_asset(
        blob_resource: AzBlobResource,
    ) -> MaterializeResult:
        def _read_cerp_cookies(cerp_cookies_path: Path) -> list[Cookie]:
            with cerp_cookies_path.open("rb") as f:
                cerp_cookies = pickle.load(f)

            return cerp_cookies

        def _download_cerp() -> bytes:
            cerp_cookies = _read_cerp_cookies(cerp_cookies_path)
            output_id, _ = _read_output_timestamp(timestamps_path, cerp_mapping)

            session = requests.Session()
            for cookie in cerp_cookies:
                session.cookies.set(cookie["name"], cookie["value"])
            download_response = session.get(output_download_url.format(output_id))

            return download_response.content

        def _upload_blob(
            blob_resource: AzBlobResource,
            download_content: bytes,
        ) -> tuple[str, str]:
            container_name = dagster_env.get_value()
            _, job_timestamp = _read_output_timestamp(timestamps_path, cerp_mapping)
            job_timestamp_date = job_timestamp[:10]
            blob_name = f"{job_timestamp_date}/{asset_name}.csv"

            blob_client = blob_resource.get_blob_service_client().get_blob_client(
                container=container_name,
                blob=blob_name,
            )
            blob_client.upload_blob(
                download_content,
                encoding="utf-8",
                overwrite=True,
            )

            return (container_name, blob_name)

        download_content = _download_cerp()
        (container_name, blob_name) = _upload_blob(blob_resource, download_content)

        return MaterializeResult(
            metadata={
                "Container Name": container_name,
                "Blob Name": blob_name,
            }
        )

    return _cerp_blob_asset


def define_cerp_src_asset(
    cerp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"cerp_src__{cerp_mapping['name']}"
    upstream_asset_name = f"cerp_blob__{cerp_mapping['name']}"

    @asset(
        key_prefix="cerp",
        group_name="cerp_src",
        name=asset_name,
        compute_kind="sql",
        deps=[["cerp", upstream_asset_name]],
        tags={"status": cerp_mapping["status"]},
    )
    def _cerp_src_asset(
        fusion_resource: AzSQLResource,
    ) -> MaterializeResult:
        target_table = cerp_mapping["target"]
        container_name = dagster_env.get_value()
        _, job_timestamp = _read_output_timestamp(timestamps_path, cerp_mapping)
        job_timestamp_date = job_timestamp[:10]
        blob_name = f"{job_timestamp_date}/{upstream_asset_name}.csv"
        sql = f"""
            EXEC dagster_bulk_insert_azure_blob_lf
                '{target_table}',
                '{container_name}',
                '{blob_name}';
        """

        with fusion_resource.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

        return MaterializeResult(metadata={"Table Name": cerp_mapping["target"]})

    return _cerp_src_asset
