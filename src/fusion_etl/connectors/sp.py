import time

import polars as pl
import requests
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
        scope: list[str] = ["https://graph.microsoft.com/.default"],
        endpoint: str = "https://graph.microsoft.com/v1.0",
        site_id: str = "unhcr365.sharepoint.com,83a72114-cab8-4705-ad78-c5690bc82720,aa195260-deb5-445d-844c-f2b7a1ba6685",
    ):
        print("initializing Sharepoint Connector")
        self.email = credentials.email
        self.password = credentials.password
        self.totp_counter = credentials.totp_counter
        self.headless_flag = headless_flag
        self.app = PublicClientApplication(client_id)
        self.scope = scope
        self.endpoint = endpoint
        self.site_id = site_id
        self.account = None
        self.access_token = None
        print("... done")

    def download(self, etl_mappings: list[dict[str, str]]) -> list[dict[str, str]]:
        print("acquiring Sharepoint access token")
        self._get_access_token()
        print("... done")
        for etl_mapping in etl_mappings:
            if etl_mapping["source_type"] == "file":
                etl_mapping["filename"] = f"temp/{etl_mapping['source_name']}"
                self._download_file(etl_mapping)
            elif etl_mapping["source_type"] == "list":
                etl_mapping["filename"] = f"temp/{etl_mapping['source_name']}.csv"
                self._download_list(etl_mapping)
        return etl_mappings

    def _get_access_token(self):
        if not self.account:
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
                page.get_by_placeholder("Email, phone, or Skype").fill(self.email)
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

    def _download_file(self, etl_mapping: dict[str, str]):
        print(f"downloading file {etl_mapping['source_name']} from Sharepoint")
        file_url = f"{self.endpoint}/sites/{self.site_id}/drive/root:{etl_mapping['source_path'] + etl_mapping['source_name']}"
        authorization_header = {"Authorization": f"Bearer {self.access_token}"}
        download_url = self._get_download_url(file_url, authorization_header)
        self._write_to_file(download_url, authorization_header, etl_mapping)
        print("... done")

    def _get_download_url(self, file_url: str, authorization_header: str) -> str:
        with requests.get(file_url, headers=authorization_header) as r:
            download_url = r.json()["@microsoft.graph.downloadUrl"]
        return download_url

    def _write_to_file(
        self,
        download_url: str,
        authorization_header: str,
        etl_mapping: dict[str, str],
    ):
        with requests.get(download_url, headers=authorization_header) as r:
            with open(etl_mapping["filename"], "wb") as f:
                f.write(r.content)

    def _download_list(self, etl_mapping: dict[str, str]):
        print(f"downloading list {etl_mapping['source_name']} from Sharepoint")
        rows = self._get_rows(etl_mapping)
        column_name_mapping = self._get_column_name_mapping(etl_mapping)
        columns_to_download = list(column_name_mapping.keys())
        df = (
            pl.from_dicts(rows, infer_schema_length=None)
            .select(columns_to_download)
            .rename(column_name_mapping)
        )
        df.write_csv(etl_mapping["filename"])
        print("... done")

    def _get_rows(self, etl_mapping: dict[str, str]) -> list[dict[str, str]]:
        items_url = f"{self.endpoint}/sites/{self.site_id}/lists/{etl_mapping['source_name']}/items?expand=fields"
        authorization_header = {"Authorization": f"Bearer {self.access_token}"}
        with requests.get(items_url, headers=authorization_header) as r:
            items = r.json()["value"]
        rows = [item["fields"] for item in items]
        return rows

    def _get_column_name_mapping(self, etl_mapping: dict[str, str]) -> dict[str, str]:
        columns_url = f"{self.endpoint}/sites/{self.site_id}/lists/{etl_mapping['source_name']}/columns"
        authorization_header = {"Authorization": f"Bearer {self.access_token}"}
        with requests.get(columns_url, headers=authorization_header) as r:
            columns = r.json()["value"]
        column_name_mapping = {
            column["name"]: column["displayName"]
            for column in columns
            if column["name"][:5] in ["Title", "field", "Alloc"]
        }
        return column_name_mapping
