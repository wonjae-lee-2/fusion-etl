from azure.storage.blob import BlobServiceClient
from dagster import ConfigurableResource


class AzBlobResource(ConfigurableResource):
    blob_storage_url: str
    storage_access_key: str

    def get_blob_service_client(self) -> BlobServiceClient:
        blob_service_client = BlobServiceClient(
            account_url=self.blob_storage_url,
            credential=self.storage_access_key,
        )
        return blob_service_client
