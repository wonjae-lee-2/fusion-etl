import json

import pyotp


class Credentials:
    def __init__(self, path: str):
        print("initializing Credentials")
        self.path = path
        self.unhcr = self._read_credentials("unhcr")
        self.fusion = self._read_credentials("fusion")
        self.totp_counter = self._get_totp_counter()
        print("...done")

    def _read_credentials(self, key: str) -> dict[str, str]:
        with open(self.path, mode="r") as f:
            content = f.read()
            credentials = json.loads(content)
            credentials_value = credentials[key]
        return credentials_value

    def _get_totp_counter(self) -> pyotp.TOTP:
        totp_counter = pyotp.TOTP(self.unhcr["secret_key"])
        return totp_counter


def read_etl_mappings(path: str) -> list[dict[str, str]]:
    print("reading ETL mappings")
    with open(path, mode="r") as f:
        content = f.read()
        etl_mappings = json.loads(content)
    print("...done")
    return etl_mappings
