import polars as pl

from fusion_etl.connectors import otbi, sql_server


def _transform_file(download_filename: str) -> pl.DataFrame:
    df = (
        pl.read_csv(download_filename, infer_schema_length=0)
        .with_columns(pl.col("Cost").cast(pl.Float64))
        .with_columns(pl.col("Purchase Order Committed Cost").cast(pl.Float64))
    )
    return df


if __name__ == "__main__":
    secrets_filename = "secrets.json"
    report_path = "/users/leew@unhcr.org/03 situation allocation/Expenditure_2023"

    otbi_connector = otbi.Connector(secrets_filename)
    (playwright, browser, context, page) = otbi_connector.start_playwright(
        headless_flag=False
    )
    download_filename = otbi_connector.download(page, report_path)
    otbi_connector.stop_playwright(playwright, browser, context)

    df = _transform_file(download_filename)
    schema_name = "tmp"
    table_name = "Test_Expenditure_2023"

    sql_server_connector = sql_server.Connector(secrets_filename)
    cnxn = sql_server_connector.open_connection()
    sql_server_connector.insert_df(df, cnxn, schema_name, table_name)
    sql_server_connector.close_connection(cnxn)
