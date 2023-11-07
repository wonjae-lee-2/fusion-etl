import requests
from msal import PublicClientApplication
from playwright.sync_api import Page

from fusion_etl.utils import Secrets, page_in_playwright


class Connector:
    def __init__(
        self,
        secrets: Secrets,
    ) -> None:
        self.secrets = secrets

    def download(
        self,
        folder: str,
        filename: str,
        client_id: str = "04b07795-8ddb-461a-bbee-02f9e1bf7b46",
        scope: list[str] = ["https://graph.microsoft.com/.default"],
        endpoint: str = "https://graph.microsoft.com/v1.0",
        site_id: str = "unhcr365.sharepoint.com,83a72114-cab8-4705-ad78-c5690bc82720,aa195260-deb5-445d-844c-f2b7a1ba6685",
        headless_flag: bool = True,
    ) -> str:
        access_token = self._get_access_token(
            client_id,
            scope,
            headless_flag,
        )
        filename = self._download_from_sharepoint(
            access_token,
            folder,
            filename,
            endpoint,
            site_id,
        )
        return filename

    def _get_access_token(
        self,
        client_id: str,
        scope: list[str],
        headless_flag: bool,
    ) -> str:
        app = PublicClientApplication(client_id)
        device_flow = app.initiate_device_flow(scope)
        with page_in_playwright(headless_flag) as page:
            self._authenticate_on_page(page, device_flow)
        token = app.acquire_token_by_device_flow(device_flow)
        return token["access_token"]

    def _authenticate_on_page(
        self,
        page: Page,
        device_flow: dict[str, str],
    ) -> None:
        page.goto(device_flow["verification_uri"])
        page.get_by_placeholder("Code").fill(device_flow["user_code"])
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

    def _download_from_sharepoint(
        self,
        access_token: str,
        folder: str,
        filename: str,
        endpoint: str,
        site_id: str,
    ) -> str:
        file_url = f"{endpoint}/sites/{site_id}/drive/root:{folder}/{filename}"
        header = {"Authorization": f"Bearer {access_token}"}
        with requests.get(file_url, headers=header) as r:
            download_url = r.json()["@microsoft.graph.downloadUrl"]
        with requests.get(download_url, headers=header) as r:
            with open(filename, "wb") as f:
                f.write(r.content)
        return filename
