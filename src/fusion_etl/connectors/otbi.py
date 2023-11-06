from urllib.parse import quote

from playwright.sync_api import Page, sync_playwright

from fusion_etl import utils


class Connector:
    def __init__(
        self,
        unhcr_secrets: dict[str, str] = utils.read_secrets()["unhcr"],
    ) -> None:
        self.unhcr_secrets = unhcr_secrets

    def download(
        self,
        report_path: str,
        download_format: str,
    ) -> str:
        encoded_path = quote(report_path, safe="")
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()

            page = self._authenticate_on_page(page, encoded_path, download_format)
            filename = self._download_from_page(page)

            context.close()
            browser.close()
        return filename

    def _authenticate_on_page(
        self,
        page: Page,
        encoded_path: str,
        download_format: str,
    ) -> Page:
        page.goto(
            f"https://fa-esrv-saasfaprod1.fa.ocs.oraclecloud.com/analytics/saw.dll?Go&Path={encoded_path}&Action=Download&format={download_format}"
        )
        page.get_by_role("button", name="Company Single Sign-On").click()
        page.get_by_label("username@unhcr.org").fill(self.unhcr_secrets["email"])
        page.get_by_role("button", name="Next").click()
        page.locator("#i0118").fill(self.unhcr_secrets["password"])
        page.get_by_role("button", name="Sign in").click()

        totp = utils.get_totp(self.unhcr_secrets["secret_key"])

        page.get_by_placeholder("Code").fill(totp)
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
