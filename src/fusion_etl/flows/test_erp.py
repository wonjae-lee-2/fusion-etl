import polars as pl
from fusion_etl import utils
from fusion_etl.connectors import erp, fusion
from prefect import flow, task


@flow(log_prints=True)
def test_erp(
    credentials_path: str = "config/credentials.json",
    etl_mappings_path: str = "config/test_erp.json",
    headless_flag: bool = True,
):
    # _ = input("Disconnect from VPN and press enter\n")
    credentials = utils.Credentials(credentials_path)
    etl_mappings_for_download = utils.read_etl_mappings(etl_mappings_path)
    erp_connector = erp.Connector(credentials, headless_flag=headless_flag)
    etl_mappings_for_upload = download_erp(erp_connector, etl_mappings_for_download)
    # _ = input("Connect to VPN and press enter\n")
    fusion_connector = fusion.Connector(credentials, headless_flag=headless_flag)
    fusion_connector.open_conn()
    upload_erp(fusion_connector, etl_mappings_for_upload)
    fusion_connector.close_conn()


@task
def download_erp(
    erp_connector: erp.Connector,
    etl_mappings_for_download: list[dict[str, str]],
) -> list[dict[str, str]]:
    etl_mappings_for_upload = erp_connector.download(etl_mappings_for_download)
    return etl_mappings_for_upload


@task
def upload_erp(
    fusion_connector: fusion.Connector,
    etl_mappings_for_upload: list[dict[str, str]],
):
    for etl_mapping in etl_mappings_for_upload:
        df = _read_file(etl_mapping)
        fusion_connector.insert_df(etl_mapping, df)


def _read_file(etl_mapping: dict[str, str]) -> pl.DataFrame:
    print(f"reading {etl_mapping['filename']}")
    df = pl.read_csv(etl_mapping["filename"], infer_schema_length=0)
    if etl_mapping["source_name"] == "_BC_BALANCES":
        transformed_df = _transform_BC_BALANCES(df)
    elif etl_mapping["source_name"] == "FND_VS_VALUES":
        transformed_df = df
    print("...done")
    return transformed_df


def _transform_BC_BALANCES(df: pl.DataFrame) -> pl.DataFrame:
    transformed_df = (
        df.with_columns(pl.col("Total Budget").cast(pl.Float64))
        .with_columns(pl.col("Commitments").cast(pl.Float64))
        .with_columns(pl.col("Obligations").cast(pl.Float64))
        .with_columns(pl.col("Expenditures").cast(pl.Float64))
        .with_columns(pl.col("Other Anticipated Expenditures").cast(pl.Float64))
    )
    return transformed_df


if __name__ == "__main__":
    test_erp()
