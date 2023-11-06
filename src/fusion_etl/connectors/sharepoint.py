import pyotp
import requests
from msal import PublicClientApplication
from playwright.sync_api import Playwright, sync_playwright

from fusion_etl import secrets


def get_access_token(
    client_id: str = "04b07795-8ddb-461a-bbee-02f9e1bf7b46",
    scope: list = ["https://graph.microsoft.com/.default"],
) -> str:
    app = PublicClientApplication(client_id)
    device_flow = app.initiate_device_flow(scope)
    unhcr_secrets = secrets.read_secrets()["unhcr"]

    with sync_playwright() as playwright:
        login(playwright, device_flow, unhcr_secrets)

    token = app.acquire_token_by_device_flow(device_flow)
    return token["access_token"]


def login(playwright: Playwright, device_flow: dict, unhcr_secrets: dict) -> None:
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    page.goto(device_flow["verification_uri"])
    page.get_by_placeholder("Code").fill(device_flow["user_code"])
    page.get_by_role("button", name="Next").click()
    page.get_by_label("Enter your email, phone, or Skype.").fill(unhcr_secrets["email"])
    page.get_by_role("button", name="Next").click()
    page.get_by_placeholder("Password").fill(unhcr_secrets["password"])
    page.get_by_placeholder("Password").press("Enter")
    totp = pyotp.TOTP(unhcr_secrets["secret_key"])
    page.get_by_placeholder("Code").fill(totp.now())
    page.get_by_role("button", name="Verify").click()
    page.get_by_role("button", name="Continue").click()

    context.close()
    browser.close()


def download_file(
    access_token: str,
    folder: str,
    filename: str,
    endpoint: str = "https://graph.microsoft.com/v1.0",
    site_id: str = "unhcr365.sharepoint.com,83a72114-cab8-4705-ad78-c5690bc82720,aa195260-deb5-445d-844c-f2b7a1ba6685",
) -> str:
    file_url = f"{endpoint}/sites/{site_id}/drive/root:{folder}{filename}"
    header = {"Authorization": f"Bearer {access_token}"}
    with requests.get(file_url, headers=header) as r:
        download_url = r.json()["@microsoft.graph.downloadUrl"]
    with requests.get(download_url, headers=header) as r:
        with open(filename, "wb") as f:
            f.write(r.content)
    return filename
