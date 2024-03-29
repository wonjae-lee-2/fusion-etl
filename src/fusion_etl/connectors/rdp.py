import struct
import time

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
        server: str = "unhcr-rdp-prd-sql-server.database.windows.net",
        database: str = "unhcr-rdp-prd-sql-db",
        conn_attribute: int = 1256,  # SQL_COPT_SS_ACCESS_TOKEN
    ):
        print("initializing RDP Connector")
        self.email = credentials.email
        self.password = credentials.password
        self.totp_counter = credentials.totp_counter
        self.headless_flag = headless_flag
        self.app = PublicClientApplication(client_id)
        self.account = None
        self.access_token = None
        self.refresh_token = None
        self.scope = scope
        self.driver = driver
        self.server = server
        self.database = database
        self.conn = None
        self.cursor = None
        self.conn_attribute = conn_attribute
        print("... done")

    def open_conn(self) -> str:
        print("opening connection to RDP")
        self._get_access_token()
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
        print("... done")
        return self.refresh_token

    def query(
        self, etl_mapping: dict[str, str]
    ) -> tuple[list[str], list[tuple[any, ...]]]:
        print(
            f"querying {etl_mapping['source_path']}.{etl_mapping['source_name']} in RDP"
        )
        start_time = time.time()
        if etl_mapping["source_type"] == "table":
            (column_names, rows) = self._query_table(etl_mapping)
        end_time = time.time()
        duration = round(end_time - start_time)
        print(f"... done in {duration}s")
        return (column_names, rows)

    def close_conn(self):
        print("closing connection to RDP")
        self.cursor.close()
        self.conn.close()
        print("... done")

    def _get_access_token(self):
        if self.account is None:
            self._run_device_flow()
        else:
            self._use_account()

    def _run_device_flow(self):
        flow = self.app.initiate_device_flow(self.scope)
        with sync_playwright() as playwright:
            self._authenticate(playwright, flow)
        time.sleep(3)
        response = self.app.acquire_token_by_device_flow(flow)
        self.access_token = response["access_token"]
        self.refresh_token = response["refresh_token"]
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

    def _query_table(
        self, etl_mapping: dict[str, str]
    ) -> tuple[list[str], list[tuple[any, ...]]]:
        self.cursor.execute(
            f"SELECT * FROM [{etl_mapping['source_path']}].[{etl_mapping['source_name']}]"
        )
        column_names = self._get_column_names()
        rows = self.cursor.fetchall()
        return (column_names, rows)

    def _get_column_names(self) -> list[str]:
        column_names = []
        columns = self.cursor.description
        for col in columns:
            column_names.append(col[0])
        return column_names
