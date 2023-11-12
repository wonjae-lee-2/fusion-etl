import polars as pl
import pyodbc


class Connector:
    def __init__(
        self,
        sql_server_credentials: dict[str, str],
        driver: str = "ODBC Driver 18 for SQL Server",
    ):
        self.connstring = self._get_connstring(driver, sql_server_credentials)
        self.connection = None

    def _get_connstring(
        self,
        driver: str,
        sql_server_credentials: dict[str, str],
    ) -> str:
        connstring = ";".join(
            [
                f"DRIVER={driver}",
                f"SERVER={sql_server_credentials['server']}",
                f"DATABASE={sql_server_credentials['database']}",
                f"UID={sql_server_credentials['uid']}",
                f"PWD={sql_server_credentials['pwd']}",
            ]
        )
        return connstring

    def open_connection(self):
        self.connection = pyodbc.connect(self.connstring)

    def close_connection(self):
        self.connection.close()

    def insert_df(
        self,
        df: pl.DataFrame,
        schema_name: str,
        table_name: str,
    ):
        (column_names, values) = self._convert_df(df)
        with self.connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE [{schema_name}].[{table_name}]")
            cursor.fast_executemany = True
            cursor.executemany(
                f"INSERT INTO [{schema_name}].[{table_name}] ({column_names}) VALUES ({', '.join(['?' for _ in values[0]])})",
                values,
            )
            cursor.commit()

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
        return ", ".join(clean_column_names)
