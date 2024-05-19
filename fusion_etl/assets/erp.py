import json
import pickle
from pathlib import Path

import requests
from dagster import AssetsDefinition, EnvVar, MaterializeResult, asset
from playwright.sync_api import Cookie

from ..resources.azure import AzureBlobResource
from ..resources.erp import ERPResource
from ..resources.fusion import FusionResource

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
output_download_url = EnvVar("OUTPUT_DOWNLOAD_URL").get_value()


def _read_erp_timestamps(timestamps_path: Path) -> dict[str, dict[str, str]]:
    if not timestamps_path.is_file():
        raise FileNotFoundError(
            f"timestamps.json was not found at {timestamps_path}. Please run the erp timestamp sensor first."
        )

    with open(timestamps_path, "r") as f:
        timestamps: dict = json.load(f)
    erp_timestamps: dict[str, dict[str, str]] = timestamps.get("erp")

    if not erp_timestamps:
        raise ValueError(
            "The erp timestamp was not found in timestamps.json. Please run the erp timestamp sensor first."
        )

    return erp_timestamps


def _read_output_timestamp(
    timestamps_path: Path,
    erp_mapping: dict[str, str],
) -> tuple[str, str]:
    erp_timestamps = _read_erp_timestamps(timestamps_path)

    output_id = erp_timestamps.get(erp_mapping["source"]).get("output_id")
    job_timestamp = erp_timestamps.get(erp_mapping["source"]).get("job_timestamp")

    return (output_id, job_timestamp)


def define_blob_erp_asset(
    erp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"blob_erp__{erp_mapping['name']}"

    @asset(
        key_prefix="erp",
        group_name="blob_erp",
        name=asset_name,
        compute_kind="python",
    )
    def _blob_erp_asset(
        erp_resource: ERPResource,
        azure_blob_resource: AzureBlobResource,
    ) -> MaterializeResult:
        def _read_erp_cookies(erp_cookies_path: Path) -> list[Cookie]:
            with erp_cookies_path.open("rb") as f:
                erp_cookies = pickle.load(f)

            return erp_cookies

        def _download_erp() -> bytes:
            erp_cookies = _read_erp_cookies(erp_cookies_path)
            output_id, _ = _read_output_timestamp(timestamps_path, erp_mapping)

            session = requests.Session()
            for cookie in erp_cookies:
                session.cookies.set(cookie["name"], cookie["value"])
            download_response = session.get(output_download_url.format(output_id))

            return download_response.content

        def _upload_blob(
            azure_blob_resource: AzureBlobResource,
            download_content: bytes,
        ) -> tuple[str, str]:
            container_name = dagster_env.get_value()
            _, job_timestamp = _read_output_timestamp(timestamps_path, erp_mapping)
            job_timestamp_date = job_timestamp[:10]
            blob_name = f"{job_timestamp_date}/{asset_name}.csv"

            blob_client = azure_blob_resource.get_blob_service_client().get_blob_client(
                container=container_name,
                blob=blob_name,
            )
            blob_client.upload_blob(
                download_content,
                encoding="utf-8",
                overwrite=True,
            )

            return (container_name, blob_name)

        download_content = _download_erp()
        (container_name, blob_name) = _upload_blob(
            azure_blob_resource, download_content
        )

        return MaterializeResult(
            metadata={
                "Container Name": container_name,
                "Blob Name": blob_name,
            }
        )

    return _blob_erp_asset


def define_src_erp_asset(
    erp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"src_erp__{erp_mapping['name']}"
    upstream_asset_name = f"blob_erp__{erp_mapping['name']}"

    @asset(
        key_prefix="erp",
        group_name="src_erp",
        name=asset_name,
        compute_kind="sql",
        deps=[["erp", upstream_asset_name]],
    )
    def _src_erp_asset(
        fusion_resource: FusionResource,
    ) -> MaterializeResult:
        target_table = erp_mapping["target"]
        container_name = dagster_env.get_value()
        _, job_timestamp = _read_output_timestamp(timestamps_path, erp_mapping)
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

        return MaterializeResult(metadata={"Table Name": erp_mapping["target"]})

    return _src_erp_asset
