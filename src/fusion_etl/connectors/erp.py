from urllib.parse import quote

from playwright._impl._errors import TimeoutError
from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright

from fusion_etl.utils import Credentials


class Connector:
    def __init__(
        self,
        credentials: Credentials,
        headless_flag: bool = True,
        base_url: str = "https://fa-esrv-saasfaprod1.fa.ocs.oraclecloud.com",
    ):
        print("initializing Cloud ERP Connector")
        self.email = credentials.email
        self.password = credentials.password
        self.totp_counter = credentials.totp_counter
        self.headless_flag = headless_flag
        self.base_url = base_url
        print("...done")

    def download(self, etl_mappings: list[dict[str, str]]) -> list[dict[str, str]]:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless_flag)
            context, authenticated_page = self._authenticate(browser)
            etl_mappings_with_filename = self._download(
                authenticated_page,
                etl_mappings,
            )
            context.close()
            browser.close()
        return etl_mappings_with_filename

    def _authenticate(self, browser: Browser) -> tuple[BrowserContext, Page]:
        max_retries = 3
        for _ in range(max_retries):
            try:
                print("authenticating with Cloud ERP")
                context = browser.new_context()
                page = context.new_page()
                home_url = f"{self.base_url}/analytics/saw.dll?bieehome"
                page.goto(home_url)
                page.get_by_role("button", name="Company Single Sign-On").click()
                page.get_by_placeholder("username@unhcr.org").fill(self.email)
                page.get_by_role("button", name="Next").click()
                page.get_by_placeholder("Password").fill(self.password)
                page.get_by_role("button", name="Sign in").click()
                page.get_by_placeholder("Code").fill(self.totp_counter.now())
                page.get_by_role("button", name="Verify").click()
                page.get_by_role("button", name="Yes").click()
                page.goto(home_url)
                print("... done")
                return (context, page)
            except TimeoutError:
                print("... TimeoutError")
        else:
            print("... failed")

    def _download(
        self,
        authenticated_page: Page,
        etl_mappings: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        for etl_mapping in etl_mappings:
            print(
                f"downloading {etl_mapping['source_type']} {etl_mapping['source_name']} from Cloud ERP"
            )
            authenticated_page.reload()
            self._add_download_url(etl_mapping)
            self._add_filename(etl_mapping)
            api_request_context = authenticated_page.request
            r = api_request_context.get(etl_mapping["download_url"], timeout=600_000)
            with open(etl_mapping["filename"], "wb") as f:
                f.write(r.body())
            print("...done")
        return etl_mappings

    def _add_download_url(self, etl_mapping: dict[str, str]):
        match etl_mapping["source_type"]:
            case "analysis":
                etl_mapping["download_url"] = self._get_analysis_download_url(
                    etl_mapping
                )
            case "report":
                etl_mapping["download_url"] = self._get_report_download_url(etl_mapping)

    def _get_analysis_download_url(self, etl_mapping: dict[str, str]) -> str:
        encoded_path = quote(
            etl_mapping["source_path"] + etl_mapping["source_name"],
            safe="",
        )
        download_url = "&".join(
            [
                f"{self.base_url}/analytics/saw.dll?Go",
                f"Path={encoded_path}",
                "Action=Download",
                "Format=csv",
            ]
        )
        return download_url

    def _get_report_download_url(self, etl_mapping: dict[str, str]) -> str:
        encoded_path = (
            etl_mapping["source_path"] + etl_mapping["source_name"]
        ).replace(" ", "+")
        download_url = (
            f"{self.base_url}/xmlpserver"
            + f"{encoded_path}.xdo"
            + "?_xpt=1"
            + "&_xf=csv"
        )
        return download_url

    def _add_filename(self, etl_mapping: dict[str, str]):
        etl_mapping["filename"] = f"temp/{etl_mapping['source_name']}.csv"
