import csv
import io
from datetime import datetime

from dagster import AssetsDefinition, EnvVar, MaterializeResult, asset

from ..resources.azblob import AzBlobResource
from ..resources.azsql import AzSQLResource
from ..resources.sp import SharepointResource


def define_sp_blob_asset(
    sp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"sp_blob__{sp_mapping['name']}"

    @asset(
        key_prefix="sp",
        group_name="sp_blob",
        name=asset_name,
        compute_kind="azure",
        tags={"type": sp_mapping["type"]},
    )
    def _sp_blob_asset(
        sharepoint_resource: SharepointResource,
        blob_resource: AzBlobResource,
    ) -> MaterializeResult:
        def _download_sp(
            sharepoint_resource: SharepointResource,
        ) -> bytes | list[dict]:
            if sp_mapping["type"] == "file":
                return sharepoint_resource.download_file(sp_mapping)
            elif sp_mapping["type"] == "list":
                return sharepoint_resource.download_list(sp_mapping)

        def _upload_blob(
            blob_resource: AzBlobResource,
            download_content: bytes | list[dict],
        ) -> tuple[str, str]:
            container_name = dagster_env.get_value()
            timestamp = datetime.today().strftime("%Y-%m-%d")
            blob_name = f"{timestamp}/{asset_name}.csv"
            blob_client = blob_resource.get_blob_service_client().get_blob_client(
                container=container_name,
                blob=blob_name,
            )

            if sp_mapping["type"] == "file":
                upload_content = download_content
            elif sp_mapping["type"] == "list":
                with io.StringIO(newline="") as buffer:
                    column_names = download_content[0].keys()
                    writer = csv.DictWriter(buffer, fieldnames=column_names)
                    writer.writeheader()
                    writer.writerows(download_content)
                    upload_content = buffer.getvalue()

            blob_client.upload_blob(
                upload_content,
                encoding="utf-8",
                overwrite=True,
            )

            return (container_name, blob_name)

        download_content = _download_sp(sharepoint_resource)
        (container_name, blob_name) = _upload_blob(blob_resource, download_content)

        return MaterializeResult(
            metadata={
                "Container Name": container_name,
                "Blob Name": blob_name,
            }
        )

    return _sp_blob_asset


def define_sp_src_asset(
    sp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"sp_src__{sp_mapping['name']}"
    upstream_asset_name = f"sp_blob__{sp_mapping['name']}"

    @asset(
        key_prefix="sp",
        group_name="sp_src",
        name=asset_name,
        compute_kind="sql",
        deps=[["sp", upstream_asset_name]],
        tags={"type": sp_mapping["type"]},
    )
    def _sp_src_asset(
        fusion_resource: AzSQLResource,
    ) -> MaterializeResult:
        target_table = sp_mapping["target"]
        container_name = dagster_env.get_value()
        timestamp = datetime.today().strftime("%Y-%m-%d")
        blob_name = f"{timestamp}/{upstream_asset_name}.csv"
        sql = f"""
            EXEC dagster_bulk_insert_azure_blob_lf
                '{target_table}',
                '{container_name}',
                '{blob_name}';
        """

        with fusion_resource.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

        return MaterializeResult(metadata={"Table Name": sp_mapping["target"]})

    return _sp_src_asset
