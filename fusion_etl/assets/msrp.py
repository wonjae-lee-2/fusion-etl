import csv
import io

from dagster import AssetsDefinition, EnvVar, MaterializeResult, asset

from ..resources.azure import AzureBlobResource
from ..resources.fusion import FusionResource
from ..resources.msrp import MSRPResource


def define_msrp_blob_asset(
    msrp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"msrp_blob__{msrp_mapping['name']}"

    @asset(
        key_prefix="msrp",
        group_name="msrp_blob",
        name=asset_name,
        compute_kind="azure",
    )
    def _msrp_blob_asset(
        msrp_resource: MSRPResource,
        azure_blob_resource: AzureBlobResource,
    ) -> MaterializeResult:
        def _query_msrp(msrp_resource: MSRPResource) -> list[tuple[int | str, ...]]:
            source_table = msrp_mapping["source"]
            sql = f"""
                SELECT * FROM {source_table};
            """

            with msrp_resource.connect() as conn:
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
            blob_name = f"msrp/{asset_name}.csv"

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

        rows_with_header = _query_msrp(msrp_resource)
        (container_name, blob_name) = _upload_blob(
            azure_blob_resource, rows_with_header
        )

        return MaterializeResult(
            metadata={
                "Container Name": container_name,
                "Blob Name": blob_name,
            }
        )

    return _msrp_blob_asset


def define_msrp_src_asset(
    msrp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"msrp_src__{msrp_mapping['name']}"
    upstream_asset_name = f"msrp_blob__{msrp_mapping['name']}"

    @asset(
        key_prefix="msrp",
        group_name="msrp_src",
        name=asset_name,
        compute_kind="sql",
        deps=[["msrp", upstream_asset_name]],
    )
    def _msrp_src_asset(
        fusion_resource: FusionResource,
    ) -> MaterializeResult:
        target_table = msrp_mapping["target"]
        container_name = dagster_env.get_value()
        blob_name = f"msrp/{upstream_asset_name}.csv"
        sql = f"""
            EXEC dagster_bulk_insert_azure_blob
                '{target_table}',
                '{container_name}',
                '{blob_name}';
        """

        with fusion_resource.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

        return MaterializeResult(metadata={"Table Name": msrp_mapping["target"]})

    return _msrp_src_asset
