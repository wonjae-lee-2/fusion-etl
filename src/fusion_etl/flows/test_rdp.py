import polars as pl
from prefect import flow, task

from fusion_etl import utils
from fusion_etl.connectors import fusion, rdp


@flow(log_prints=True)
def test_rdp(
    credentials_path: str = "config/credentials.json",
    etl_mappings_path: str = "config/test_rdp.json",
    headless_flag: bool = True,
):
    # _ = input("Connect to VPN and press enter\n")
    credentials = utils.Credentials(credentials_path)
    etl_mappings = utils.read_etl_mappings(etl_mappings_path)
    rdp_connector = rdp.Connector(credentials, headless_flag=headless_flag)
    refresh_token = rdp_connector.open_conn()
    fusion_connector = fusion.Connector(credentials, headless_flag=headless_flag)
    fusion_connector.open_conn(refresh_token)
    copy_rdp(rdp_connector, fusion_connector, etl_mappings)
    rdp_connector.close_conn()
    fusion_connector.close_conn()


@task
def copy_rdp(
    rdp_connector: rdp.Connector,
    fusion_connector: fusion.Connector,
    etl_mappings: list[dict[str, str]],
):
    for etl_mapping in etl_mappings:
        (column_names, rows) = rdp_connector.query(etl_mapping)
        if etl_mapping["source_name"] == "UDGPIndicatorAGDPopulationType":
            df = _transform_UDGPIndicatorAGDPopulationType(column_names, rows)
            fusion_connector.insert_df(etl_mapping, df)
        else:
            fusion_connector.insert_rows(etl_mapping, column_names, rows)


def _transform_UDGPIndicatorAGDPopulationType(
    column_names: list[str],
    rows: list[tuple[any, ...]],
) -> pl.DataFrame:
    data = [dict(zip(column_names, row)) for row in rows]
    df = (
        pl.DataFrame(data=data, infer_schema_length=None)
        .with_columns(pl.col("ImpactStatement").cast(pl.Utf8).fill_null(""))
        .with_columns(pl.col("OutcomeStatement").cast(pl.Utf8).fill_null(""))
        .with_columns(pl.col("OutputStatement").cast(pl.Utf8).fill_null(""))
    )
    return df


if __name__ == "__main__":
    test_rdp()
