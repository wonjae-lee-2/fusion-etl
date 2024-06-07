import requests
from azure.identity import AzureCliCredential
from dagster import ConfigurableResource, EnvVar

power_bi_history_uri = EnvVar("POWER_BI_HISTORY_URI").get_value()
power_bi_query_uri = EnvVar("POWER_BI_QUERY_URI").get_value()


class PowerBIResource(ConfigurableResource):
    power_bi_credential_scope: str
    group_id: str
    dataset_id: str

    def execute(self, dax: str) -> list[dict]:
        dataset_query_uri = power_bi_query_uri.format(
            self.group_id,
            self.dataset_id,
        )
        authorization_header = self._get_authorization_header()
        payload = {
            "queries": [{"query": dax}],
            "serializerSettings": {"includeNulls": True},
        }
        r = requests.post(dataset_query_uri, headers=authorization_header, json=payload)
        rows = r.json()["results"][0]["tables"][0]["rows"]

        return rows

    def _get_authorization_header(self) -> dict[str, str]:
        token = AzureCliCredential().get_token(self.power_bi_credential_scope)
        authorization_header = {"Authorization": f"Bearer {token.token}"}
        return authorization_header
