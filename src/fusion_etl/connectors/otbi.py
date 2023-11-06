from urllib.parse import quote

import pyotp
from playwright.sync_api import Playwright, sync_playwright

from fusion_etl import settings


def download_csv(path: str) -> str:
    encoded_path = quote(path, safe="")
    unhcr_secrets = settings.read_secrets()["unhcr"]

    with sync_playwright() as playwright:
        filename = _login_and_download(playwright, encoded_path, unhcr_secrets)
    return filename


def _login_and_download(
    playwright: Playwright,
    encoded_path: str,
    unhcr_secrets: dict,
) -> str:
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    page.goto(
        f"https://fa-esrv-saasfaprod1.fa.ocs.oraclecloud.com/analytics/saw.dll?Go&Path={encoded_path}&Action=Download&format=csv"
    )
    page.get_by_role("button", name="Company Single Sign-On").click()
    page.get_by_label("username@unhcr.org").fill(unhcr_secrets["email"])
    page.get_by_role("button", name="Next").click()
    page.locator("#i0118").fill(unhcr_secrets["password"])
    page.get_by_role("button", name="Sign in").click()
    totp = pyotp.TOTP(unhcr_secrets["secret_key"])
    page.get_by_placeholder("Code").fill(totp.now())
    page.get_by_role("button", name="Verify").click()
    with page.expect_download(timeout=0) as download_info:
        page.get_by_role("button", name="Yes").click()
        download = download_info.value
    download.save_as(download.suggested_filename)

    context.close()
    browser.close()
    return download.suggested_filename
