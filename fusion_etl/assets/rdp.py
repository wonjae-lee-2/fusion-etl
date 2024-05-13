import csv
import datetime
import io

from dagster import AssetsDefinition, MaterializeResult, asset, EnvVar

from ..resources.azure import AzureBlobResource
from ..resources.fusion import FusionResource
from ..resources.rdp import RDPResource


def define_blob_rdp_asset(
    rdp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"blob_rdp__{rdp_mapping['name']}"

    @asset(
        key_prefix="blob",
        group_name="blob_rdp",
        name=asset_name,
        compute_kind="python",
    )
    def _blob_rdp_asset(
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
            utc_today = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
            blob_name = f"{utc_today}/{asset_name}.csv"

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

    return _blob_rdp_asset


def define_src_rdp_asset(
    rdp_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"src_rdp__{rdp_mapping['name']}"
    upstream_asset_name = f"blob_rdp__{rdp_mapping['name']}"

    @asset(
        key_prefix="src",
        group_name="src_rdp",
        name=asset_name,
        compute_kind="sql",
        deps=[["blob", upstream_asset_name]],
    )
    def _src_rdp_asset(
        fusion_resource: FusionResource,
    ) -> MaterializeResult:
        target_table = rdp_mapping["target"]
        container_name = dagster_env.get_value()
        utc_today = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
        blob_name = f"{utc_today}/{upstream_asset_name}.csv"
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

    return _src_rdp_asset
