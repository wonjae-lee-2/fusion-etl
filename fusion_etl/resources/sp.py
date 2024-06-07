import requests
from azure.identity import AzureCliCredential
from dagster import ConfigurableResource, EnvVar

sharepoint_file_uri = EnvVar("SHAREPOINT_FILE_URI").get_value()
sharepoint_list_rows_uri = EnvVar("SHAREPOINT_LIST_ROWS_URI").get_value()
sharepoint_list_columns_uri = EnvVar("SHAREPOINT_LIST_COLUMNS_URI").get_value()


class SharepointResource(ConfigurableResource):
    ms_graph_credential_scope: str
    paru_site_id: str

    def download_file(self, mapping: dict[str, str]) -> bytes:
        site_file_uri = sharepoint_file_uri.format(self.paru_site_id, mapping["source"])
        authorization_header = self._get_authorization_header()
        file_download_url = self._get_download_url(site_file_uri, authorization_header)

        with requests.get(file_download_url, headers=authorization_header) as r:
            file = r.content

        return file

    def download_list(self, mapping: dict[str, str]):
        rows = self._get_rows(mapping)
        column_mapping = self._get_column_mapping(mapping)
        list = [
            {column_mapping[k]: v for k, v in row.items() if k in column_mapping.keys()}
            for row in rows
        ]

        return list

    def _get_authorization_header(self) -> dict[str, str]:
        token = AzureCliCredential().get_token(self.ms_graph_credential_scope)
        authorization_header = {"Authorization": f"Bearer {token.token}"}

        return authorization_header

    def _get_download_url(self, site_file_uri: str, authorization_header: str) -> str:
        with requests.get(site_file_uri, headers=authorization_header) as r:
            file_download_url = r.json()["@microsoft.graph.downloadUrl"]

        return file_download_url

    def _get_rows(self, mapping: dict[str, str]) -> list[dict]:
        site_list_rows_uri = sharepoint_list_rows_uri.format(
            self.paru_site_id, mapping["source"]
        )
        authorization_header = self._get_authorization_header()

        with requests.get(site_list_rows_uri, headers=authorization_header) as r:
            items = r.json()["value"]
        rows = [item["fields"] for item in items]

        return rows

    def _get_column_mapping(self, mapping: dict[str, str]) -> dict:
        site_list_columns_uri = sharepoint_list_columns_uri.format(
            self.paru_site_id, mapping["source"]
        )
        authorization_header = self._get_authorization_header()

        with requests.get(site_list_columns_uri, headers=authorization_header) as r:
            columns = r.json()["value"]

        column_mapping = {
            column["name"]: column["displayName"]
            for column in columns
            if column["name"][:5] in ["Title", "field", "Alloc"]
        }

        return column_mapping
