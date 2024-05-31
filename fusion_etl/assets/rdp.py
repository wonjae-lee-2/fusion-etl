import csv
import io
import json
from pathlib import Path

from dagster import AssetsDefinition, EnvVar, MaterializeResult, asset

from ..resources.azure import AzureBlobResource
from ..resources.fusion import FusionResource
from ..resources.rdp import RDPResource


def _get_rdp_timestamp_date() -> str:
    timestamps_path = (
        Path(EnvVar("SQLITE_STORAGE_BASE_DIR").get_value())
        .joinpath("timestamps.json")
        .resolve()
    )

    if not timestamps_path.is_file():
        raise FileNotFoundError(
            f"timestamps.json was not found at {timestamps_path}. Please run the rdp timestamp sensor first."
        )

    with open(timestamps_path, "r") as f:
        timestamps: dict[str, str] = json.load(f)
    rdp_timestamp = timestamps.get("rdp")

    if not rdp_timestamp:
        raise ValueError(
            "The rdp timestamp was not found in timestamps.json. Please run the rdp timestamp sensor first."
        )

    return rdp_timestamp[:10]


def define_rdp_blob_asset(
    rdp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"rdp_blob__{rdp_mapping['name']}"

    @asset(
        key_prefix="rdp",
        group_name="rdp_blob",
        name=asset_name,
        compute_kind="azure",
    )
    def _rdp_blob_asset(
        rdp_resource: RDPResource,
        azure_blob_resource: AzureBlobResource,
    ) -> MaterializeResult:
        def _query_rdp(rdp_resource: RDPResource) -> list[tuple[int | str, ...]]:
            source_table = rdp_mapping["source"]
            sql = f"""
                SELECT * FROM {source_table};
            """

            with rdp_resource.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    header = tuple(column[0] for column in cursor.description)
                    rows = cursor.fetchall()
                    rows_with_header = [header] + rows

            return rows_with_header

        def _upload_blob(
            azure_blob_resource: AzureBlobResource,
            rows_with_header: list[tuple[int | str, ...]],
        ) -> tuple[str | None, str]:
            container_name = dagster_env.get_value()
            rdp_timestamp_date = _get_rdp_timestamp_date()
            blob_name = f"{rdp_timestamp_date}/{asset_name}.csv"

            with io.StringIO(newline="") as buffer:
                writer = csv.writer(buffer)
                writer.writerows(rows_with_header)
                blob_client = (
                    azure_blob_resource.get_blob_service_client().get_blob_client(
                        container=container_name,
                        blob=blob_name,
                    )
                )
                blob_client.upload_blob(
                    buffer.getvalue(),
                    encoding="utf-8",
                    overwrite=True,
                )

            return (container_name, blob_name)

        rows_with_header = _query_rdp(rdp_resource)
        (container_name, blob_name) = _upload_blob(
            azure_blob_resource, rows_with_header
        )

        return MaterializeResult(
            metadata={
                "Container Name": container_name,
                "Blob Name": blob_name,
            }
        )

    return _rdp_blob_asset


def define_rdp_src_asset(
    rdp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"rdp_src__{rdp_mapping['name']}"
    upstream_asset_name = f"rdp_blob__{rdp_mapping['name']}"

    @asset(
        key_prefix="rdp",
        group_name="rdp_src",
        name=asset_name,
        compute_kind="sql",
        deps=[["rdp", upstream_asset_name]],
    )
    def _rdp_src_asset(
        fusion_resource: FusionResource,
    ) -> MaterializeResult:
        target_table = rdp_mapping["target"]
        container_name = dagster_env.get_value()
        rdp_timestamp_date = _get_rdp_timestamp_date()
        blob_name = f"{rdp_timestamp_date}/{upstream_asset_name}.csv"
        sql = f"""
            EXEC dagster_bulk_insert_azure_blob
                '{target_table}',
                '{container_name}',
                '{blob_name}';
        """

        with fusion_resource.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

        return MaterializeResult(metadata={"Table Name": rdp_mapping["target"]})

    return _rdp_src_asset
