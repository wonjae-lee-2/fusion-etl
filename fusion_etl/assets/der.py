import csv
import io
import json
import re
from pathlib import Path

from dagster import AssetsDefinition, EnvVar, MaterializeResult, asset

from ..resources.azblob import AzBlobResource
from ..resources.azsql import AzSQLResource
from ..resources.pbi import PowerBIResource


def _get_der_timestamp_date() -> str:
    timestamps_path = (
        Path(EnvVar("SQLITE_STORAGE_BASE_DIR").get_value())
        .joinpath("timestamps.json")
        .resolve()
    )

    if not timestamps_path.is_file():
        raise FileNotFoundError(
            f"timestamps.json was not found at {timestamps_path}. Please run the der timestamp sensor first."
        )

    with open(timestamps_path, "r") as f:
        timestamps: dict[str, str] = json.load(f)
    der_timestamp = timestamps.get("der")

    if not der_timestamp:
        raise ValueError(
            "The der timestamp was not found in timestamps.json. Please run the der timestamp sensor first."
        )

    return der_timestamp[:10]


def define_der_blob_asset(
    der_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"der_blob__{der_mapping['name']}"

    @asset(
        key_prefix="der",
        group_name="der_blob",
        name=asset_name,
        compute_kind="azure",
    )
    def _der_blob_asset(
        der_resource: PowerBIResource,
        blob_resource: AzBlobResource,
    ) -> MaterializeResult:
        def _get_row_count(der_resource: PowerBIResource) -> int:
            source_table = der_mapping["source"]
            dax = f"""
                EVALUATE
                ROW("Row_Count", COUNTROWS({source_table}))
            """

            row_count = der_resource.execute(dax)
            return row_count[0]["[Row_Count]"]

        def _get_column_count(der_resource: PowerBIResource) -> int:
            source_table = der_mapping["source"]
            dax = f"""
                EVALUATE
                TOPNSKIP(1, 0, {source_table})
            """

            first_row = der_resource.execute(dax)
            column_count = len(first_row[0])
            return column_count

        def _extract_column_name(key: str) -> str:
            match = re.search(r"\[(.*?)\]", key)
            column_name = match.group(1) if match else key
            return column_name

        def _query_dataset(der_resource: PowerBIResource) -> list[dict]:
            source_table = der_mapping["source"]
            dax = """
                EVALUATE
                TOPNSKIP({}, {}, {})
            """
            all_rows = []
            row_count = _get_row_count(der_resource)
            column_count = _get_column_count(der_resource)
            if column_count > 150:
                rows_to_fetch = 1_500
            else:
                rows_to_fetch = 15_000

            for i in range(0, row_count, rows_to_fetch):
                rows = der_resource.execute(dax.format(rows_to_fetch, i, source_table))
                clean_rows = [
                    {_extract_column_name(k): v for k, v in row.items()} for row in rows
                ]
                all_rows.extend(clean_rows)

            try:
                assert len(all_rows) == row_count
            except AssertionError:
                raise AssertionError(
                    f"Expected {row_count} rows, but got {len(all_rows)} rows."
                )

            return all_rows

        def _upload_blob(
            blob_resource: AzBlobResource,
            rows: list[dict],
        ) -> tuple[str | None, str]:
            container_name = dagster_env.get_value()
            der_timestamp_date = _get_der_timestamp_date()
            blob_name = f"{der_timestamp_date}/{asset_name}.csv"

            with io.StringIO(newline="") as buffer:
                column_names = rows[0].keys()
                writer = csv.DictWriter(buffer, fieldnames=column_names)
                writer.writeheader()
                writer.writerows(rows)
                blob_client = blob_resource.get_blob_service_client().get_blob_client(
                    container=container_name,
                    blob=blob_name,
                )
                blob_client.upload_blob(
                    buffer.getvalue(),
                    encoding="utf-8",
                    overwrite=True,
                )

            return (container_name, blob_name)

        all_rows = _query_dataset(der_resource)
        (container_name, blob_name) = _upload_blob(blob_resource, all_rows)

        return MaterializeResult(
            metadata={
                "Container Name": container_name,
                "Blob Name": blob_name,
            }
        )

    return _der_blob_asset


def define_der_src_asset(
    der_mapping: dict[str, str],
    dagster_env: EnvVar,
) -> AssetsDefinition:
    asset_name = f"der_src__{der_mapping['name']}"
    upstream_asset_name = f"der_blob__{der_mapping['name']}"

    @asset(
        key_prefix="der",
        group_name="der_src",
        name=asset_name,
        compute_kind="sql",
        deps=[["der", upstream_asset_name]],
    )
    def _der_src_asset(
        fusion_resource: AzSQLResource,
    ) -> MaterializeResult:
        target_table = der_mapping["target"]
        container_name = dagster_env.get_value()
        der_timestamp_date = _get_der_timestamp_date()
        blob_name = f"{der_timestamp_date}/{upstream_asset_name}.csv"
        sql = f"""
            EXEC dagster_bulk_insert_azure_blob
                '{target_table}',
                '{container_name}',
                '{blob_name}';
        """

        with fusion_resource.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

        return MaterializeResult(metadata={"Table Name": der_mapping["target"]})

    return _der_src_asset
