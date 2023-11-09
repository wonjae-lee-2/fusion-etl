import polars as pl

from fusion_etl.connectors import sharepoint, sql_server


def _transform_file(sharepoint_filename: str) -> pl.DataFrame:
    df = pl.read_csv(sharepoint_filename, infer_schema_length=0).with_columns(
        pl.col("Modified").str.to_datetime(format="%d/%m/%Y %H:%M").alias("Modified")
    )
    return df


if __name__ == "__main__":
    secrets_filename = "secrets.json"
    sharepoint_folder = "/BI Team/04. Processes/Fusion ETL"
    sharepoint_filename = "cost_center_mapping.csv"

    sharepoint_connector = sharepoint.Connector(secrets_filename)
    sharepoint_connector.download_file(
        sharepoint_folder, sharepoint_filename, headless_flag=False
    )

    df = _transform_file(sharepoint_filename)
    schema_name = "tmp"
    table_name = "Test_CostCenterMapping"

    sql_server_connector = sql_server.Connector(secrets_filename)
    cnxn = sql_server_connector.open_connection()
    sql_server_connector.insert_df(df, cnxn, schema_name, table_name)
    sql_server_connector.close_connection(cnxn)
