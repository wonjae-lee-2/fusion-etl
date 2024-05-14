from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import dbt
from .assets.mappings import RDP_MAPPINGS
from .assets.rdp import define_blob_rdp_asset, define_src_rdp_asset
from .resources.azure import AzureBlobResource
from .resources.fusion import FusionResource
from .resources.rdp import RDPResource

blob_rdp_assets = [
    define_blob_rdp_asset(rdp_mapping, EnvVar("DAGSTER_ENV"))
    for rdp_mapping in RDP_MAPPINGS
]
source_rdp_assets = [
    define_src_rdp_asset(rdp_mapping, EnvVar("DAGSTER_ENV"))
    for rdp_mapping in RDP_MAPPINGS
]
dbt_assets = load_assets_from_modules([dbt])

azure_blob_resource = AzureBlobResource(
    account_url=EnvVar("AZURE_STORAGE_URL"),
    credential=EnvVar("AZURE_STORAGE_ACCESS_KEY"),
)
rdp_resource = RDPResource(
    azure_database_credential_scope=EnvVar("AZURE_DATABASE_CREDENTIAL_SCOPE"),
    odbc_driver=EnvVar("ODBC_DRIVER"),
    rdp_server=EnvVar("RDP_SERVER"),
    rdp_database=EnvVar("RDP_DATABASE"),
)
fusion_resource = FusionResource(
    azure_database_credential_scope=EnvVar("AZURE_DATABASE_CREDENTIAL_SCOPE"),
    odbc_driver=EnvVar("ODBC_DRIVER"),
    fusion_server=EnvVar("FUSION_SERVER"),
    fusion_database=EnvVar("FUSION_DATABASE"),
)
dbt_project_dir = EnvVar("DBT_PROJECT_DIR").get_value()
dbt_resource = DbtCliResource(project_dir=dbt_project_dir)

defs = Definitions(
    assets=[*blob_rdp_assets, *source_rdp_assets, *dbt_assets],
    resources={
        "azure_blob_resource": azure_blob_resource,
        "rdp_resource": rdp_resource,
        "fusion_resource": fusion_resource,
        "dbt_resource": dbt_resource,
    },
)
