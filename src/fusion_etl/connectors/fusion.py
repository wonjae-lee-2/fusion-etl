import os
import struct
import time

import polars as pl
import pyodbc
from msal import PublicClientApplication
from playwright._impl._errors import TimeoutError
from playwright.sync_api import Browser, Playwright, sync_playwright

from fusion_etl.utils import Credentials


class Connector:
    def __init__(
        self,
        credentials: Credentials,
        headless_flag: bool = True,
        client_id: str = "04b07795-8ddb-461a-bbee-02f9e1bf7b46",
        scope: list[str] = ["https://database.windows.net/.default"],
        driver: str = "ODBC Driver 18 for SQL Server",
        server: str = "smi-hub-paru-prod.8ab996bfb1ef.database.windows.net",
        database: str = "fusion",
        conn_attribute: int = 1256,  # SQL_COPT_SS_ACCESS_TOKEN
    ):
        print("initializing Fusion DB Connector")
        self.email = credentials.email
        self.password = credentials.password
        self.totp_counter = credentials.totp_counter
        self.headless_flag = headless_flag
        self.app = PublicClientApplication(client_id)
        self.account = None
        self.access_token = None
        self.scope = scope
        self.driver = driver
        self.server = server
        self.database = database
        self.conn = None
        self.cursor = None
        self.conn_attribute = conn_attribute
        print("... done")

    def open_conn(self, refresh_token: str | None = None):
        print("opening connection to Fusion DB")
        self._get_access_token(refresh_token)
        connstring = f"""
            Driver={self.driver};
            Server={self.server};
            Database={self.database};
        """
        sql_server_token = self._get_sql_server_token()
        self.conn = pyodbc.connect(
            connstring,
            attrs_before={self.conn_attribute: sql_server_token},
        )
        self.cursor = self.conn.cursor()
        self.cursor.fast_executemany = True
        print("... done")

    def insert_rows(
        self,
        etl_mapping: dict[str, str],
        column_names: list[str],
        rows: list[tuple[any, ...]],
    ):
        target_schema = etl_mapping["target_schema"]
        target_table = etl_mapping["target_table"]
        print(f"inserting into {target_schema}.{target_table} in Fusion DB")
        start_time = time.time()
        if not rows:
            print("... no rows to insert")
        else:
            column_names_str = self._join_column_names(column_names)
            self.cursor.execute(f"TRUNCATE TABLE [{target_schema}].[{target_table}]")
            self.cursor.executemany(
                f"INSERT INTO [{target_schema}].[{target_table}] ({column_names_str}) VALUES ({', '.join(['?' for _ in rows[0]])})",
                rows,
            )
            self.cursor.commit()
        end_time = time.time()
        duration = round(end_time - start_time)
        print(f"... done in {duration}s")

    def insert_df(
        self,
        etl_mapping: dict[str, str],
        df: pl.DataFrame,
    ):
        target_schema = etl_mapping["target_schema"]
        target_table = etl_mapping["target_table"]
        print(f"inserting into {target_schema}.{target_table} in Fusion DB")
        start_time = time.time()
        if df.is_empty():
            print("... no dataframe to insert")
        else:
            (column_names, values) = self._convert_df(df)
            self.cursor.execute(f"TRUNCATE TABLE [{target_schema}].[{target_table}]")
            self.cursor.executemany(
                f"INSERT INTO [{target_schema}].[{target_table}] ({column_names}) VALUES ({', '.join(['?' for _ in values[0]])})",
                values,
            )
            self.cursor.commit()
        if "filename" in etl_mapping:
            os.remove(etl_mapping["filename"])
        end_time = time.time()
        duration = round(end_time - start_time)
        print(f"... done in {duration}s")

    def close_conn(self):
        print("closing connection to Fusion DB")
        self.cursor.close()
        self.conn.close()
        print("... done")

    def _get_access_token(self, refresh_token: str | None):
        if refresh_token is not None:
            self._use_refresh_token(refresh_token)
        elif not self.account:
            self._run_device_flow()
        else:
            self._use_account()

    def _use_refresh_token(self, refresh_token: str):
        response = self.app.acquire_token_by_refresh_token(refresh_token, self.scope)
        self.access_token = response["access_token"]

    def _run_device_flow(self):
        flow = self.app.initiate_device_flow(self.scope)
        with sync_playwright() as playwright:
            self._authenticate(playwright, flow)
        time.sleep(3)
        response = self.app.acquire_token_by_device_flow(flow)
        self.access_token = response["access_token"]
        self.account = self.app.get_accounts()[0]

    def _authenticate(self, playwright: Playwright, flow: dict[str, str]):
        browser = playwright.chromium.launch(headless=self.headless_flag)
        self._try_authentication(browser, flow)
        browser.close()

    def _try_authentication(self, browser: Browser, flow: dict[str, str]):
        max_retries = 3
        for _ in range(max_retries):
            try:
                context = browser.new_context()
                page = context.new_page()
                page.goto(flow["verification_uri"])
                page.get_by_placeholder("Code").fill(flow["user_code"])
                page.get_by_role("button", name="Next").click()
                page.get_by_placeholder("Email or phone").fill(self.email)
                page.get_by_role("button", name="Next").click()
                page.get_by_placeholder("Password").fill(self.password)
                page.get_by_placeholder("Password").press("Enter")
                page.get_by_placeholder("Code").fill(self.totp_counter.now())
                page.get_by_role("button", name="Verify").click()
                page.get_by_role("button", name="Continue").click()
                context.close()
                break
            except TimeoutError:
                print("... TimeoutError")
        else:
            print("... failed")

    def _use_account(self):
        response = self.app.acquire_token_silent(self.scope, self.account)
        self.access_token = response["access_token"]

    def _get_sql_server_token(self) -> bytes:
        expanded_token = b""
        for i in bytes(self.access_token, "UTF-8"):
            expanded_token += bytes([i])
            expanded_token += bytes(1)
        sql_server_token = struct.pack("=i", len(expanded_token)) + expanded_token
        return sql_server_token

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
            .replace("/", "_")
            for column_name in df.columns
        ]
        escaped_column_names = [
            f"[{column_name}]" for column_name in clean_column_names
        ]
        column_names_str = ", ".join(escaped_column_names)
        return column_names_str
