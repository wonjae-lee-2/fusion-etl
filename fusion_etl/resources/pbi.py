import requests
from azure.identity import AzureCliCredential
from dagster import ConfigurableResource, EnvVar

power_bi_history_uri = EnvVar("POWER_BI_HISTORY_URI").get_value()
power_bi_query_uri = EnvVar("POWER_BI_QUERY_URI").get_value()


class PowerBIResource(ConfigurableResource):
    power_bi_credential_scope: str
    der_group_id: str
    der_dataset_id: str

    def get_last_timestamp(self) -> str:
        der_history_uri = power_bi_history_uri.format(
            self.der_group_id,
            self.der_dataset_id,
        )
        authorization_header = self._get_authorization_header()
        r = requests.get(der_history_uri, headers=authorization_header)
        values = r.json()["value"]
        timestamps = [
            value["endTime"] for value in values if value["status"] == "Completed"
        ]
        last_timestamp = max(timestamps)

        return last_timestamp

    def execute(self, dax: str) -> list[dict]:
        der_query_uri = power_bi_query_uri.format(
            self.der_group_id,
            self.der_dataset_id,
        )
        authorization_header = self._get_authorization_header()
        payload = {
            "queries": [{"query": dax}],
            "serializerSettings": {"includeNulls": True},
        }
        r = requests.post(der_query_uri, headers=authorization_header, json=payload)
        rows = r.json()["results"][0]["tables"][0]["rows"]

        return rows

    def _get_authorization_header(self) -> dict[str, str]:
        token = AzureCliCredential().get_token(self.power_bi_credential_scope)
        authorization_header = {"Authorization": f"Bearer {token.token}"}
        return authorization_header
