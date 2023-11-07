from contextlib import contextmanager
from urllib.parse import quote

from playwright.sync_api import Page, sync_playwright

from fusion_etl.utils import Secrets


class Connector:
    def __init__(
        self,
        secrets: Secrets,
    ) -> None:
        self.secrets = secrets

    def download(
        self,
        report_path: str,
        download_format: str = "csv",
        headless_flag: bool = True,
    ) -> str:
        action = "Download"
        encoded_path = quote(report_path, safe="")
        go_url = self._get_go_url(encoded_path, action, download_format)
        with self._page_in_playwright(headless_flag) as page:
            page = self._authenticate_on_page(page, go_url)
            filename = self._download_from_page(page)
        return filename

    def _get_go_url(
        self,
        encoded_path: str,
        action: str,
        download_format: str,
    ) -> str:
        go_url = f"https://fa-esrv-saasfaprod1.fa.ocs.oraclecloud.com/analytics/saw.dll?Go&Path={encoded_path}&Action={action}&format={download_format}"
        return go_url

    @contextmanager
    def _page_in_playwright(
        self,
        headless_flag: bool,
    ) -> Page:
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

    def _authenticate_on_page(
        self,
        page: Page,
        go_url: str,
    ) -> Page:
        page.goto(go_url)
        page.get_by_role("button", name="Company Single Sign-On").click()
        page.get_by_label("username@unhcr.org").fill(self.secrets.unhcr["email"])
        page.get_by_role("button", name="Next").click()
        page.locator("#i0118").fill(self.secrets.unhcr["password"])
        page.get_by_role("button", name="Sign in").click()
        page.get_by_placeholder("Code").fill(self.secrets.totp.now())
        page.get_by_role("button", name="Verify").click()
        return page

    def _download_from_page(
        self,
        page: Page,
    ) -> str:
        with page.expect_download(timeout=0) as download_info:
            page.get_by_role("button", name="Yes").click()
            download = download_info.value
        download.save_as(download.suggested_filename)
        return download.suggested_filename
