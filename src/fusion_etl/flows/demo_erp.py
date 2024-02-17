import polars as pl
from fusion_etl import utils
from fusion_etl.connectors import erp, fusion
from prefect import flow, task


@flow(log_prints=True)
def demo_erp(
    credentials_path: str = "config/credentials.json",
    etl_mappings_path: str = "config/demo_erp.json",
    headless_flag: bool = True,
):
    # _ = input("Disconnect from VPN and press enter\n")
    credentials = utils.Credentials(credentials_path)
    etl_mappings_download = utils.read_etl_mappings(etl_mappings_path)
    etl_mappings_upload = download_erp(
        credentials,
        etl_mappings_download,
        headless_flag,
    )
    # _ = input("Connect to VPN and press enter\n")
    upload_erp(credentials, etl_mappings_upload)


@task
def download_erp(
    credentials: utils.Credentials,
    etl_mappings: list[dict[str, str]],
    headless_flag: bool,
) -> list[dict[str, str]]:
    erp_connector = erp.Connector(credentials, headless_flag=headless_flag)
    erp_connector.start_playwright()
    for etl_mapping in etl_mappings:
        if etl_mapping["source"] == "erp":
            erp_connector.download(etl_mapping)
    erp_connector.stop_playwright()
    return etl_mappings


@task
def upload_erp(
    credentials: utils.Credentials,
    etl_mappings: list[dict[str, str]],
):
    fusion_connector = fusion.Connector(credentials)
    fusion_connector.open_conn()
    for etl_mapping in etl_mappings:
        if etl_mapping["source"] == "erp":
            df = _read_csv(etl_mapping)
            fusion_connector.insert_df(etl_mapping, df)
    fusion_connector.close_conn()


def _read_csv(etl_mapping: dict[str, str]) -> pl.DataFrame:
    print(f"reading {etl_mapping['filename']}")
    match etl_mapping["source_name"]:
        case "Test_BC_Balances_2023":
            df = _read_Test_BC_Balances_2023(etl_mapping["filename"])
        case "PPM Commitment":
            df = _read_PPM_Commitment(etl_mapping["filename"])
    print("...done")
    return df


def _read_Test_BC_Balances_2023(filename: str) -> pl.DataFrame:
    df = (
        pl.read_csv(filename, infer_schema_length=0)
        .with_columns(pl.col("Total Budget").cast(pl.Float64))
        .with_columns(pl.col("Expenditures").cast(pl.Float64))
        .with_columns(pl.col("Obligations").cast(pl.Float64))
        .with_columns(pl.col("Commitments").cast(pl.Float64))
        .with_columns(pl.col("Other Anticipated Expenditures").cast(pl.Float64))
    )
    return df


def _read_PPM_Commitment(filename: str) -> pl.DataFrame:
    df = (
        pl.read_csv(filename, infer_schema_length=0)
        .with_columns(pl.col("TIMESTAMP").str.to_datetime("%Y-%m-%d %H:%M:%S"))
        .with_columns(pl.col("PA_DATE").str.to_date("%Y-%m-%d"))
        .with_columns(pl.col("USD_AMOUNT").cast(pl.Float64))
    )
    return df


if __name__ == "__main__":
    demo_erp()
