import json


def read_secrets(filename: str = "secrets.json") -> dict:
    with open(filename, mode="r") as f:
        content = f.read()
        secrets = json.loads(content)
    return secrets
