from contextlib import contextmanager

import requests
from msal import PublicClientApplication
from playwright.sync_api import Page, sync_playwright

from fusion_etl.utils import Secrets


class Connector:
    def __init__(self, secrets_filename: str):
        self.secrets = Secrets(secrets_filename)

    def download_file(
        self,
        sharepoint_folder: str,
        sharepoint_filename: str,
        client_id: str = "04b07795-8ddb-461a-bbee-02f9e1bf7b46",
        scope: list[str] = ["https://graph.microsoft.com/.default"],
        endpoint: str = "https://graph.microsoft.com/v1.0",
        site_id: str = "unhcr365.sharepoint.com,83a72114-cab8-4705-ad78-c5690bc82720,aa195260-deb5-445d-844c-f2b7a1ba6685",
        headless_flag: bool = True,
    ):
        access_token = self._get_access_token(
            client_id,
            scope,
            headless_flag,
        )
        self._download_file(
            access_token,
            sharepoint_folder,
            sharepoint_filename,
            endpoint,
            site_id,
        )

    def _get_access_token(
        self,
        client_id: str,
        scope: list[str],
        headless_flag: bool,
    ) -> str:
        app = PublicClientApplication(client_id)
        flow = app.initiate_device_flow(scope)
        with self._page_in_playwright(headless_flag) as page:
            self._authenticate(page, flow)
        tokens = app.acquire_token_by_device_flow(flow)
        return tokens["access_token"]

    @contextmanager
    def _page_in_playwright(self, headless_flag: bool) -> Page:
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=headless_flag)
        context = browser.new_context()
        page = context.new_page()
        try:
            yield page
        finally:
            context.close()
            browser.close()
            playwright.stop()

    def _authenticate(self, page: Page, flow: dict[str, str]):
        page.goto(flow["verification_uri"])
        page.get_by_placeholder("Code").fill(flow["user_code"])
        page.get_by_role("button", name="Next").click()
        page.get_by_label("Enter your email, phone, or Skype.").fill(
            self.secrets.unhcr["email"]
        )
        page.get_by_role("button", name="Next").click()
        page.get_by_placeholder("Password").fill(self.secrets.unhcr["password"])
        page.get_by_placeholder("Password").press("Enter")
        page.get_by_placeholder("Code").fill(self.secrets.totp.now())
        page.get_by_role("button", name="Verify").click()
        page.get_by_role("button", name="Continue").click()

    def _download_file(
        self,
        access_token: str,
        sharepoint_folder: str,
        sharepoint_filename: str,
        endpoint: str,
        site_id: str,
    ):
        file_url = f"{endpoint}/sites/{site_id}/drive/root:{sharepoint_folder}/{sharepoint_filename}"
        header = {"Authorization": f"Bearer {access_token}"}
        with requests.get(file_url, headers=header) as r:
            download_url = r.json()["@microsoft.graph.downloadUrl"]
        with requests.get(download_url, headers=header) as r:
            with open(sharepoint_filename, "wb") as f:
                f.write(r.content)
