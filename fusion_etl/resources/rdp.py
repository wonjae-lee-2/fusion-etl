import struct
from itertools import chain, repeat

import pyodbc
from azure.core.credentials import AccessToken
from azure.identity import AzureCliCredential
from dagster import ConfigurableResource


class RDPResource(ConfigurableResource):
    azure_database_credential_scope: str
    odbc_driver: str
    rdp_server: str
    rdp_database: str

    def connect(self) -> pyodbc.Connection:
        attrs_before = self._get_pyodbc_attrs_before()
        conn_str = f"""
                Driver={self.odbc_driver};
                Server={self.rdp_server};
                Database={self.rdp_database};
            """
        conn = pyodbc.connect(
            conn_str,
            attrs_before=attrs_before,
        )
        conn.add_output_converter(
            pyodbc.SQL_BIT,
            lambda x: int.from_bytes(
                x,
                byteorder="big",
            ),
        )
        return conn

    def _get_pyodbc_attrs_before(self) -> dict[int, bytes]:
        token = self._get_cli_access_token()
        token_bytes = self._convert_access_token_to_mswindows_byte_string(token)
        sql_copt_ss_access_token = 1256
        attrs_before = {sql_copt_ss_access_token: token_bytes}
        return attrs_before

    def _get_cli_access_token(self) -> AccessToken:
        token = AzureCliCredential().get_token(self.azure_database_credential_scope)
        return token

    def _convert_access_token_to_mswindows_byte_string(
        self,
        token: AccessToken,
    ) -> bytes:
        value = bytes(token.token, "UTF-8")
        encoded_bytes = bytes(chain.from_iterable(zip(value, repeat(0))))
        mswindows_byte_string = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
        return mswindows_byte_string
