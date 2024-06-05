import pickle
from pathlib import Path

import pyotp
from dagster import ConfigurableResource, EnvVar
from playwright.sync_api import Browser, BrowserContext, Cookie, sync_playwright

erp_cookies_path = (
    Path(EnvVar("SQLITE_STORAGE_BASE_DIR").get_value())
    .joinpath("erp_cookies.pkl")
    .resolve()
)
totp_counter = pyotp.TOTP(EnvVar("TOTP_SECRET_KEY").get_value())


class BIPublisherResource(ConfigurableResource):
    oracle_analytics_publisher_url: str
    unhcr_email: str
    unhcr_password: str

    def refresh_cookies(self) -> list[Cookie]:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            context = self._authenticate(browser)
            erp_cookies = context.cookies()
            with erp_cookies_path.open("wb") as f:
                pickle.dump(erp_cookies, f)
            context.close()
            browser.close()

        return erp_cookies

    def _authenticate(self, browser: Browser) -> BrowserContext:
        context = browser.new_context()
        page = context.new_page()
        page.goto(self.oracle_analytics_publisher_url)
        page.get_by_role("button", name="Company Single Sign-On").click()
        page.get_by_placeholder("username@unhcr.org").fill(self.unhcr_email)
        page.get_by_role("button", name="Next").click()
        page.get_by_placeholder("Password").fill(self.unhcr_password)
        page.get_by_role("button", name="Sign in").click()
        page.get_by_placeholder("Code").fill(totp_counter.now())
        page.get_by_role("button", name="Verify").click()
        page.get_by_role("button", name="Yes").click()
        page.wait_for_load_state("domcontentloaded")
        page.goto(self.oracle_analytics_publisher_url)
        return context
