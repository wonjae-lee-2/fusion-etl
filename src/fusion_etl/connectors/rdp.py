import struct
import time

import pyodbc
import pyotp
from msal import PublicClientApplication
from playwright.sync_api import Page, Playwright, sync_playwright


class Connector:
    def __init__(
        self,
        unhcr_credentials: dict[str, str],
        totp_counter: pyotp.TOTP,
        etl_mappings: list[dict[str, str]],
        headless_flag: bool,
        client_id: str = "04b07795-8ddb-461a-bbee-02f9e1bf7b46",
        scope: list[str] = ["https://database.windows.net/.default"],
        driver: str = "ODBC Driver 18 for SQL Server",
        server: str = "unhcr-rdp-prd-sql-server.database.windows.net",
        database: str = "unhcr-rdp-prd-sql-db",
    ):
        self.unhcr_credentials = unhcr_credentials
        self.totp_counter = totp_counter
        self.etl_mappings = etl_mappings
        self.headless_flag = headless_flag
        self.app = PublicClientApplication(client_id)
        self.scope = scope
        self.driver = driver
        self.server = server
        self.database = database
        self.account = None
        self.access_token = None

    def download(self):
        for etl_mapping in self.etl_mappings:
            self._get_access_token()
            match etl_mapping["source_type"]:
                case "table":
                    etl_mapping["filename"] = f"{etl_mapping['source_name']}.csv"
                    self._query_table(etl_mapping)
        return self.etl_mappings

    def _get_access_token(self):
        if self.account is None:
            self._run_device_flow()
        else:
            self._use_account()

    def _run_device_flow(self):
        flow = self.app.initiate_device_flow(self.scope)
        with sync_playwright() as playwright:
            self._authenticate_with_playwright(playwright, flow)
        time.sleep(5)
        response = self.app.acquire_token_by_device_flow(flow)
        self.access_token = response["access_token"]
        self.account = self.app.get_accounts()[0]

    def _authenticate_with_playwright(
        self, playwright: Playwright, flow: dict[str, str]
    ):
        browser = playwright.chromium.launch(headless=self.headless_flag)
        context = browser.new_context()
        page = context.new_page()

        self._authenticate(page, flow)

        context.close()
        browser.close()

    def _authenticate(self, page: Page, flow: dict[str, str]):
        page.goto(flow["verification_uri"])
        page.get_by_placeholder("Code").fill(flow["user_code"])
        page.get_by_role("button", name="Next").click()
        page.get_by_label("Enter your email, phone, or Skype.").fill(
            self.unhcr_credentials["email"]
        )
        page.get_by_role("button", name="Next").click()
        page.get_by_placeholder("Password").fill(self.unhcr_credentials["password"])
        page.get_by_placeholder("Password").press("Enter")
        page.get_by_placeholder("Code").fill(self.totp_counter.now())
        page.get_by_role("button", name="Verify").click()
        page.get_by_role("button", name="Continue").click()

    def _use_account(self):
        response = self.app.acquire_token_silent(self.scope, self.account)
        self.access_token = response["access_token"]

    def _query_table(self, etl_mapping: dict[str, str]):
        tokenstruct = self._get_tokenstruct(self.access_token)
        connstring = f"""
            Driver={self.driver};
            Server={self.server};
            Database={self.database};
        """
        conn = pyodbc.connect(connstring, attrs_before={1256: tokenstruct})
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT * FROM {etl_mapping['source_path']}.{etl_mapping['source_name']}"
        )
        rows = cursor.fetchall()
        return rows

    def _get_tokenstruct(self, access_token: str):
        exptoken = b""
        for i in bytes(access_token, "UTF-8"):
            exptoken += bytes({i})
            exptoken += bytes(1)
        tokenstruct = struct.pack("=i", len(exptoken)) + exptoken
        return tokenstruct
