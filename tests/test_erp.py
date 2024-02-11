import json
import os

import pytest
from fusion_etl.connectors import erp
from fusion_etl.utils import Credentials


@pytest.fixture
def credentials_path():
    return "config/credentials.json"


@pytest.fixture
def credentials(credentials_path):
    return Credentials(credentials_path)


@pytest.fixture
def unhcr_credentials(credentials):
    return credentials.unhcr


@pytest.fixture
def totp_counter(credentials):
    return credentials.totp_counter


@pytest.fixture
def etl_mappings_path():
    return "config/demo_otbi.json"


@pytest.fixture
def etl_mappings(etl_mappings_path):
    with open(etl_mappings_path, mode="r") as f:
        content = f.read()
        etl_mappings = json.loads(content)
    return etl_mappings


@pytest.fixture
def headless_flag():
    return True


def test_download(unhcr_credentials, totp_counter, etl_mappings, headless_flag):
    connector = erp.Connector(
        unhcr_credentials,
        totp_counter,
        etl_mappings,
        headless_flag,
    )
    etl_mappings = connector.download()
    for etl_mapping in etl_mappings:
        assert os.path.exists(etl_mapping["filename"])
        os.remove(etl_mapping["filename"])
