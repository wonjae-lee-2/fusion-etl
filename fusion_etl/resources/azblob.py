from azure.storage.blob import BlobServiceClient
from dagster import ConfigurableResource


class AzBlobResource(ConfigurableResource):
    account_url: str
    credential: str

    def get_blob_service_client(self) -> BlobServiceClient:
        blob_service_client = BlobServiceClient(
            account_url=self.account_url,
            credential=self.credential,
        )
        return blob_service_client
