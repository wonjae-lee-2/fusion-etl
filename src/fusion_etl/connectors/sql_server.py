import polars as pl
import pyodbc

from fusion_etl.utils import Secrets


class Connector:
    def __init__(self, secrets_filename: str):
        self.secrets = Secrets(secrets_filename)

    def open_connection(self) -> pyodbc.Connection:
        connstring = self._get_connstring()
        return pyodbc.connect(connstring)

    def _get_connstring(self) -> str:
        connstring = ";".join(
            [
                "DRIVER={ODBC Driver 18 for SQL Server}",
                f"SERVER={self.secrets.sql_server['server']}",
                f"DATABASE={self.secrets.sql_server['database']}",
                f"UID={self.secrets.sql_server['uid']}",
                f"PWD={self.secrets.sql_server['pwd']}",
            ]
        )
        return connstring

    def close_connection(self, connection: pyodbc.Connection):
        connection.close()

    def insert_df(
        self,
        df: pl.DataFrame,
        cnxn: pyodbc.Connection,
        schema_name: str,
        table_name: str,
    ):
        (columns, values) = self._convert_df(df)
        with cnxn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE [{schema_name}].[{table_name}]")
            cursor.fast_executemany = True
            cursor.executemany(
                f"INSERT INTO [{schema_name}].[{table_name}] ({columns}) VALUES ({', '.join(['?' for _ in values[0]])})",
                values,
            )
            cursor.commit()

    def _convert_df(self, df: pl.DataFrame) -> tuple[str, list[tuple[any, ...]]]:
        columns = self._clean_columns(df)
        values = df.rows()
        return (columns, values)

    def _clean_columns(self, df: pl.DataFrame) -> str:
        clean_columns = [
            column.replace("(", "")
            .replace(")", "")
            .replace(" ", "_")
            .replace("-", "_")
            .replace(":", "_")
            for column in df.columns
        ]
        return ", ".join(clean_columns)
