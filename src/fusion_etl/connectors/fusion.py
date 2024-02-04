import polars as pl
import pyodbc

from fusion_etl.utils import Credentials


class Connector:
    def __init__(
        self,
        credentials: Credentials,
        driver: str = "ODBC Driver 18 for SQL Server",
    ):
        self.driver = driver
        self.fusion_credentials = credentials.fusion
        self.conn = None

    def open_conn(self):
        connstring = self._get_connstring()
        self.conn = pyodbc.connect(connstring)

    def insert_rows(
        self,
        etl_mapping: dict[str, str],
        column_names: list[str],
        rows: list[tuple[any, ...]],
    ):
        target_schema = etl_mapping["target_schema"]
        target_table = etl_mapping["target_table"]
        column_names_str = self._join_column_names(column_names)
        with self.conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE [{target_schema}].[{target_table}]")
            cursor.fast_executemany = True
            cursor.executemany(
                f"INSERT INTO [{target_schema}].[{target_table}] ({column_names_str}) VALUES ({', '.join(['?' for _ in rows[0]])})",
                rows,
            )
            cursor.commit()

    def insert_df(
        self,
        df: pl.DataFrame,
        schema_name: str,
        table_name: str,
    ):
        (column_names, values) = self._convert_df(df)
        with self.conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE [{schema_name}].[{table_name}]")
            cursor.fast_executemany = True
            cursor.executemany(
                f"INSERT INTO [{schema_name}].[{table_name}] ({column_names}) VALUES ({', '.join(['?' for _ in values[0]])})",
                values,
            )
            cursor.commit()

    def close_conn(self):
        self.conn.close()

    def _get_connstring(self) -> str:
        connstring = ";".join(
            [
                f"DRIVER={self.driver}",
                f"SERVER={self.fusion_credentials['server']}",
                f"DATABASE={self.fusion_credentials['database']}",
                f"UID={self.fusion_credentials['uid']}",
                f"PWD={self.fusion_credentials['pwd']}",
            ]
        )
        return connstring

    def _join_column_names(self, column_names: list[str]) -> str:
        escaped_column_names = [f"[{column_name}]" for column_name in column_names]
        column_names_str = ", ".join(escaped_column_names)
        return column_names_str

    def _convert_df(self, df: pl.DataFrame) -> tuple[str, list[tuple[any, ...]]]:
        column_names = self._clean_column_names(df)
        values = df.rows()
        return (column_names, values)

    def _clean_column_names(self, df: pl.DataFrame) -> str:
        clean_column_names = [
            column_name.replace("(", "")
            .replace(")", "")
            .replace(" ", "_")
            .replace("-", "_")
            .replace(":", "_")
            for column_name in df.columns
        ]
        escaped_column_names = [
            f"[{column_name}]" for column_name in clean_column_names
        ]
        column_names_str = ", ".join(escaped_column_names)
        return column_names_str
