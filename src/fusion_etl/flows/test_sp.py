import polars as pl
from prefect import flow, task

from fusion_etl import utils
from fusion_etl.connectors import fusion, sp


@flow(log_prints=True)
def test_sp(
    credentials_path: str = "config/credentials.json",
    etl_mappings_path: str = "config/test_sp.json",
    headless_flag: bool = True,
):
    # _ = input("Connect to VPN and press enter\n")
    credentials = utils.Credentials(credentials_path)
    etl_mappings_for_download = utils.read_etl_mappings(etl_mappings_path)
    sp_connector = sp.Connector(credentials, headless_flag=headless_flag)
    etl_mappings_for_upload = download_sp(sp_connector, etl_mappings_for_download)
    fusion_connector = fusion.Connector(credentials, headless_flag=headless_flag)
    fusion_connector.open_conn()
    upload_sp(fusion_connector, etl_mappings_for_upload)
    fusion_connector.close_conn()


@task
def download_sp(
    sp_connector: sp.Connector,
    etl_mappings: list[dict[str, str]],
) -> list[dict[str, str]]:
    etl_mappings_for_upload = sp_connector.download(etl_mappings)
    return etl_mappings_for_upload


@task
def upload_sp(
    fusion_connector: fusion.Connector,
    etl_mappings_for_upload: list[dict[str, str]],
):
    for etl_mapping in etl_mappings_for_upload:
        df = _read_file(etl_mapping)
        fusion_connector.insert_df(etl_mapping, df)


def _read_file(etl_mapping: dict[str, str]) -> pl.DataFrame:
    print(f"reading {etl_mapping["filename"]}")
    df = pl.read_csv(etl_mapping["filename"], infer_schema_length=0)
    if etl_mapping["source_name"] == "cost_center_mapping_final.csv":
        transformed_df = _transform_cost_center_mapping_final(df)
    elif etl_mapping["source_name"] == "Situation Allocation Ratios 2024":
        transformed_df = _transform_situation_allocation_ratios_2024(df)
    print("... done")
    return transformed_df


def _transform_cost_center_mapping_final(df: pl.DataFrame) -> pl.DataFrame:
    transformed_df = df.with_columns(
        pl.col("Modified").str.to_datetime("%d/%m/%Y %H:%M")
    )
    return transformed_df


def _transform_situation_allocation_ratios_2024(df: pl.DataFrame) -> pl.DataFrame:
    transformed_df = df.with_columns(pl.col("Allocation Ratio").cast(pl.Float64))
    return transformed_df


if __name__ == "__main__":
    test_sp()
