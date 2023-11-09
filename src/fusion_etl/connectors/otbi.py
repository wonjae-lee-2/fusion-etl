from urllib.parse import quote

from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    sync_playwright,
)

from fusion_etl.utils import Secrets


class Connector:
    def __init__(self, secrets_filename: str):
        self.secrets = Secrets(secrets_filename)

    def start_playwright(
        self,
        headless_flag: bool,
    ) -> tuple[Playwright, Browser, BrowserContext, Page]:
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=headless_flag)
        context = browser.new_context()
        page = context.new_page()
        return (playwright, browser, context, page)

    def stop_playwright(
        self,
        playwright: Playwright,
        browser: Browser,
        context: BrowserContext,
    ):
        context.close()
        browser.close()
        playwright.stop()

    def download(
        self,
        page: Page,
        report_path: str,
        action: str = "Download",
        download_format: str = "csv",
    ) -> str:
        encoded_path = quote(report_path, safe="")
        go_url = self._get_go_url(encoded_path, action, download_format)
        page = self._authenticate(page, go_url)
        download_filename = self._download(page)
        return download_filename

    def _get_go_url(
        self,
        encoded_path: str,
        action: str,
        download_format: str,
        base_url: str = "https://fa-esrv-saasfaprod1.fa.ocs.oraclecloud.com/analytics/saw.dll",
    ) -> str:
        go_url = "&".join(
            [
                f"{base_url}?Go",
                f"Path={encoded_path}",
                f"Action={action}",
                f"format={download_format}",
            ]
        )
        return go_url

    def _authenticate(self, page: Page, go_url: str) -> Page:
        page.goto(go_url)
        page.get_by_role("button", name="Company Single Sign-On").click()
        page.get_by_label("username@unhcr.org").fill(self.secrets.unhcr["email"])
        page.get_by_role("button", name="Next").click()
        page.locator("#i0118").fill(self.secrets.unhcr["password"])
        page.get_by_role("button", name="Sign in").click()
        page.get_by_placeholder("Code").fill(self.secrets.totp.now())
        page.get_by_role("button", name="Verify").click()
        return page

    def _download(self, page: Page) -> str:
        with page.expect_download(timeout=0) as download_info:
            page.get_by_role("button", name="Yes").click()
            download = download_info.value
        download.save_as(download.suggested_filename)
        return download.suggested_filename
