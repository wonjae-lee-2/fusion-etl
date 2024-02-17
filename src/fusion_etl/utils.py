import json

import pyotp


class Credentials:
    def __init__(self, path: str):
        print("initializing Credentials")
        self.email = None
        self.password = None
        self.secret_key = None
        self._read_credentials(path)
        self.totp_counter = self._get_totp_counter()
        print("...done")

    def _read_credentials(self, path: str):
        with open(path, mode="r") as f:
            content = f.read()
            credentials = json.loads(content)
        self.email = credentials["email"]
        self.password = credentials["password"]
        self.secret_key = credentials["secret_key"]

    def _get_totp_counter(self) -> pyotp.TOTP:
        totp_counter = pyotp.TOTP(self.secret_key)
        return totp_counter


def read_etl_mappings(path: str) -> list[dict[str, str]]:
    print("reading ETL mappings")
    with open(path, mode="r") as f:
        content = f.read()
        etl_mappings = json.loads(content)
    print("...done")
    return etl_mappings
