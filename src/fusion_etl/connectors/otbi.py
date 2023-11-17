import time
from urllib.parse import quote

import pyotp
from playwright.sync_api import Page, Playwright, sync_playwright


class Connector:
    def __init__(
        self,
        unhcr_credentials: dict[str, str],
        totp_counter: pyotp.TOTP,
        etl_mappings: list[dict[str, str]],
        headless_flag: bool,
        base_url: str = "https://fa-esrv-saasfaprod1.fa.ocs.oraclecloud.com",
    ):
        self.unhcr_credentials = unhcr_credentials
        self.totp_counter = totp_counter
        self.etl_mappings = etl_mappings
        self.headless_flag = headless_flag
        self.base_url = base_url
        self.home_url = f"{self.base_url}/analytics/saw.dll?bieehome"
        self.action = None
        self.download_format = None

    def download(
        self,
        action: str = "Download",
        download_format: str = "csv",
    ) -> list[dict[str, str]]:
        self.action = action
        self.download_format = download_format
        with sync_playwright() as playwright:
            self._download_with_playwright(playwright)
        return self.etl_mappings

    def _download_with_playwright(self, playwright: Playwright):
        browser = playwright.chromium.launch(headless=self.headless_flag)
        context = browser.new_context()
        page = context.new_page()

        authenticated_page = self._authenticate(page)
        time.sleep(5)
        self._download(authenticated_page)

        context.close()
        browser.close()

    def _authenticate(self, page: Page) -> Page:
        page.goto(self.home_url)
        page.get_by_role("button", name="Company Single Sign-On").click()
        page.get_by_label("username@unhcr.org").fill(self.unhcr_credentials["email"])
        page.get_by_role("button", name="Next").click()
        page.locator("#i0118").fill(self.unhcr_credentials["password"])
        page.get_by_role("button", name="Sign in").click()
        page.get_by_placeholder("Code").fill(self.totp_counter.now())
        page.get_by_role("button", name="Verify").click()
        page.get_by_role("button", name="Yes").click()
        return page

    def _download(self, authenticated_page: Page):
        for etl_mapping in self.etl_mappings:
            self._add_download_url_to_etl_mappings(etl_mapping)
            self._add_filename_to_etl_mappings(etl_mapping)
            api_request_context = authenticated_page.request
            r = api_request_context.get(etl_mapping["download_url"], timeout=600_000)
            with open(etl_mapping["filename"], "wb") as f:
                f.write(r.body())
            authenticated_page.reload()

    def _add_download_url_to_etl_mappings(self, etl_mapping: dict[str, str]):
        match etl_mapping["source_type"]:
            case "analysis":
                etl_mapping["download_url"] = self._get_download_url_for_analysis(
                    etl_mapping
                )
            case "report":
                etl_mapping["download_url"] = self._get_download_url_for_report(
                    etl_mapping
                )

    def _get_download_url_for_analysis(self, etl_mapping: dict[str, str]) -> str:
        encoded_path = quote(
            etl_mapping["source_path"] + etl_mapping["source_name"],
            safe="",
        )
        download_url = "&".join(
            [
                f"{self.base_url}/analytics/saw.dll?Go",
                f"Path={encoded_path}",
                f"Action={self.action}",
                f"format={self.download_format}",
            ]
        )
        return download_url

    def _get_download_url_for_report(self, etl_mapping: dict[str, str]) -> str:
        encoded_path = (
            etl_mapping["source_path"] + etl_mapping["source_name"]
        ).replace(" ", "+")
        download_url = (
            f"{self.base_url}/xmlpserver"
            + f"{encoded_path}.xdo"
            + "?_xpt=1"
            + f"&_xf={self.download_format}"
        )
        return download_url

    def _add_filename_to_etl_mappings(self, etl_mapping: dict[str, str]):
        etl_mapping["filename"] = f"{etl_mapping['source_name']}.{self.download_format}"
