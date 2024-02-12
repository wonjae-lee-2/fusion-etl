from urllib.parse import quote

from playwright.sync_api import Page, sync_playwright

from fusion_etl.utils import Credentials


class Connector:
    def __init__(
        self,
        credentials: Credentials,
        headless_flag: bool = True,
        base_url: str = "https://fa-esrv-saasfaprod1.fa.ocs.oraclecloud.com",
    ):
        print("initializing Cloud ERP Connector")
        self.unhcr_credentials = credentials.unhcr
        self.totp_counter = credentials.totp_counter
        self.headless_flag = headless_flag
        self.base_url = base_url
        self.playwright = None
        self.browser = None
        self.authenticated_page = None
        print("...done")

    def start_playwright(self):
        print("starting playwright")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless_flag)
        new_page = self.browser.new_page()
        self.authenticated_page = self._authenticate(new_page)
        print("...done")

    def download(self, etl_mapping: dict[str, str]):
        print(f"downloading {etl_mapping['source_name']}")
        self._add_download_url(etl_mapping)
        self._add_filename(etl_mapping)
        api_request_context = self.authenticated_page.request
        r = api_request_context.get(etl_mapping["download_url"], timeout=600_000)
        with open(etl_mapping["filename"], "wb") as f:
            f.write(r.body())
        self.authenticated_page.reload()
        print("...done")

    def stop_playwright(self):
        print("stopping playwright")
        self.browser.close()
        self.playwright.stop()
        print("...done")

    def _authenticate(self, page: Page) -> Page:
        home_url = f"{self.base_url}/analytics/saw.dll?bieehome"
        page.goto(home_url)
        page.get_by_role("button", name="Company Single Sign-On").click()
        page.get_by_placeholder("username@unhcr.org").fill(
            self.unhcr_credentials["email"]
        )
        page.get_by_role("button", name="Next").click()
        page.get_by_placeholder("Password").fill(self.unhcr_credentials["password"])
        page.get_by_role("button", name="Sign in").click()
        page.get_by_placeholder("Code").fill(self.totp_counter.now())
        page.get_by_role("button", name="Verify").click()
        page.get_by_role("button", name="Yes").click()
        return page

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
