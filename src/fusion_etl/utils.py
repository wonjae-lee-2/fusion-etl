import json

import pyotp


def read_secrets(filename: str = "secrets.json") -> dict[str, dict[str, str]]:
    with open(filename, mode="r") as f:
        content = f.read()
        secrets = json.loads(content)
    return secrets


def get_totp(secret_key: str) -> str:
    totp = pyotp.TOTP(secret_key)
    return totp.now()
