import csv
import io
from datetime import datetime

from dagster import AssetsDefinition, EnvVar, MaterializeResult, asset

from ..resources.azblob import AzBlobResource
from ..resources.azsql import AzSQLResource


def define_orion_blob_asset(
    orion_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"orion_blob__{orion_mapping['name']}"

    @asset(
        key_prefix="orion",
        group_name="orion_blob",
        name=asset_name,
        compute_kind="azure",
    )
    def _orion_blob_asset(
        orion_resource: AzSQLResource,
        blob_resource: AzBlobResource,
    ) -> MaterializeResult:
        def _upload_blob(
            blob_resource: AzBlobResource,
            rows: list[tuple[int | str, ...]],
            overwrite_flag: bool,
        ) -> tuple[str | None, str]:
            container_name = dagster_env.get_value()
            timestamp = datetime.today().strftime("%Y-%m-%d")
            blob_name = f"{timestamp}/{asset_name}.csv"

            with io.StringIO(newline="") as buffer:
                writer = csv.writer(buffer)
                writer.writerows(rows)
                blob_client = blob_resource.get_blob_service_client().get_blob_client(
                    container=container_name,
                    blob=blob_name,
                )
                blob_client.upload_blob(
                    buffer.getvalue(),
                    blob_type="AppendBlob",
                    encoding="utf-8",
                    overwrite=overwrite_flag,
                )

            return (container_name, blob_name)

        def _sync_orion(orion_resource: AzSQLResource) -> tuple[str | None, str]:
            source_table = orion_mapping["source"]
            sql = f"""
                SELECT * FROM {source_table};
            """
            rows_to_fetch = 200_000

            with orion_resource.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    header = tuple(column[0] for column in cursor.description)
                    (container_name, blob_name) = _upload_blob(
                        blob_resource,
                        [header],
                        True,
                    )
                    while True:
                        rows = cursor.fetchmany(rows_to_fetch)
                        if not rows:
                            break
                        _upload_blob(blob_resource, rows, False)

            return (container_name, blob_name)

        (container_name, blob_name) = _sync_orion(orion_resource)

        return MaterializeResult(
            metadata={
                "Container Name": container_name,
                "Blob Name": blob_name,
            }
        )

    return _orion_blob_asset


def define_orion_src_asset(
    orion_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"orion_src__{orion_mapping['name']}"
    upstream_asset_name = f"orion_blob__{orion_mapping['name']}"

    @asset(
        key_prefix="orion",
        group_name="orion_src",
        name=asset_name,
        compute_kind="sql",
        deps=[["orion", upstream_asset_name]],
    )
    def _orion_src_asset(
        fusion_resource: AzSQLResource,
    ) -> MaterializeResult:
        target_table = orion_mapping["target"]
        container_name = dagster_env.get_value()
        timestamp = datetime.today().strftime("%Y-%m-%d")
        blob_name = f"{timestamp}/{upstream_asset_name}.csv"
        sql = f"""
            EXEC dagster_bulk_insert_azure_blob
                '{target_table}',
                '{container_name}',
                '{blob_name}';
        """

        with fusion_resource.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

        return MaterializeResult(metadata={"Table Name": orion_mapping["target"]})

    return _orion_src_asset
