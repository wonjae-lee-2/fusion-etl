import json

import pyotp


class Secrets:
    def __init__(self, filename: str) -> None:
        self.unhcr = self._read_secrets(filename)["unhcr"]
        self.sql_server = self._read_secrets(filename)["sql_server"]
        self.totp = self._get_totp_generator(self.unhcr["secret_key"])

    def _read_secrets(self, filename: str) -> dict[str, dict[str, str]]:
        with open(filename, mode="r") as f:
            content = f.read()
            secrets = json.loads(content)
        return secrets

    def _get_totp_generator(self, secret_key: str) -> str:
        return pyotp.TOTP(secret_key)
