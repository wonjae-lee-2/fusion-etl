import json
import os

import polars as pl
import pyotp

from fusion_etl.connectors import otbi, sharepoint, sql_server


class Credentials:
    def __init__(self, path: str):
        self.path = path
        self.unhcr = self._read_credentials("unhcr")
        self.sql_server = self._read_credentials("sql_server")
        self.totp_counter = self._get_totp_counter()

    def _read_credentials(self, key: str) -> dict[str, str]:
        with open(self.path, mode="r") as f:
            content = f.read()
            credentials = json.loads(content)
            credentials_value = credentials[key]
        return credentials_value

    def _get_totp_counter(self) -> pyotp.TOTP:
        totp_counter = pyotp.TOTP(self.unhcr["secret_key"])
        return totp_counter


class BasePipeline:
    def __init__(
        self,
        etl_mappings_path: str,
        unhcr_network_flag: bool,
        sources: list[str] = ["otbi", "sharepoint"],
        credentials_path: str = "config/credentials.json",
        headless_flag: bool = True,
    ):
        self.credentials = Credentials(credentials_path)
        self.etl_mappings = self._read_etl_mappings(etl_mappings_path)
        self.headless_flag = headless_flag
        self.unhcr_network_flag = unhcr_network_flag
        self.sources = sources
        self.destination = sql_server.Connector(self.credentials.sql_server)

    def _read_etl_mappings(self, etl_mappings_path: str) -> list[dict[str, str]]:
        with open(etl_mappings_path, mode="r") as f:
            content = f.read()
            etl_mappings = json.loads(content)
        return etl_mappings

    def run(self):
        self._download()
        if not self.unhcr_network_flag:
            input("Connect to VPN. Press Enter to continue...")
        self.destination.open_connection()
        self._upload()
        self.destination.close_connection()

    def _download(self):
        for source in self.sources:
            etl_mappings_for_connector = [
                etl_mapping
                for etl_mapping in self.etl_mappings
                if etl_mapping["source"] == source
            ]
            if not etl_mappings_for_connector:
                continue
            connector = self._get_connector(source, etl_mappings_for_connector)
            connector.download()
        pass

    def _get_connector(
        self,
        connector_target: str,
        etl_mappings_for_connector: list[dict[str, str]],
    ) -> otbi.Connector | sharepoint.Connector:
        match connector_target:
            case "otbi":
                return otbi.Connector(
                    self.credentials.unhcr,
                    self.credentials.totp_counter,
                    etl_mappings_for_connector,
                    self.headless_flag,
                )
            case "sharepoint":
                return sharepoint.Connector(
                    self.credentials.unhcr,
                    self.credentials.totp_counter,
                    etl_mappings_for_connector,
                    self.headless_flag,
                )

    def _upload(self):
        for etl_mapping in self.etl_mappings:
            df = self._get_df(etl_mapping)
            schema_name = etl_mapping["target_schema"]
            table_name = etl_mapping["target_table"]
            self.destination.insert_df(df, schema_name, table_name)
            os.remove(etl_mapping["filename"])

    def _get_df(self, etl_mapping: dict[str, str]) -> pl.DataFrame:
        pass
