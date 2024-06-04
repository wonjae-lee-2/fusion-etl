from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import dbt
from .assets.der import define_der_blob_asset, define_der_src_asset
from .assets.der_mappings import DER_MAPPINGS
from .assets.erp import define_erp_blob_asset, define_erp_src_asset
from .assets.erp_mappings import ERP_MAPPINGS
from .assets.msrp import define_msrp_blob_asset, define_msrp_src_asset
from .assets.msrp_mappings import MSRP_MAPPINGS
from .assets.rdp import define_rdp_blob_asset, define_rdp_src_asset
from .assets.rdp_mappings import RDP_MAPPINGS
from .resources.azure import AzureBlobResource
from .resources.der import DERResource
from .resources.erp import ERPResource
from .resources.fusion import FusionResource
from .resources.msrp import MSRPResource
from .resources.rdp import RDPResource
from .sensors.dbt import dbt_run_status_sensor
from .sensors.der import der_timestamp_sensor
from .sensors.erp import erp_active_timestamp_sensor, erp_all_timestamp_sensor
from .sensors.rdp import rdp_timestamp_sensor

der_blob_assets = [
    define_der_blob_asset(der_mapping, EnvVar("DAGSTER_ENV"))
    for der_mapping in DER_MAPPINGS
]
der_source_assets = [
    define_der_src_asset(der_mapping, EnvVar("DAGSTER_ENV"))
    for der_mapping in DER_MAPPINGS
]
erp_blob_assets = [
    define_erp_blob_asset(erp_mapping, EnvVar("DAGSTER_ENV"))
    for erp_mapping in ERP_MAPPINGS
]
erp_source_assets = [
    define_erp_src_asset(erp_mapping, EnvVar("DAGSTER_ENV"))
    for erp_mapping in ERP_MAPPINGS
]
msrp_blob_assets = [
    define_msrp_blob_asset(msrp_mapping, EnvVar("DAGSTER_ENV"))
    for msrp_mapping in MSRP_MAPPINGS
]
msrp_source_assets = [
    define_msrp_src_asset(msrp_mapping, EnvVar("DAGSTER_ENV"))
    for msrp_mapping in MSRP_MAPPINGS
]
rdp_blob_assets = [
    define_rdp_blob_asset(rdp_mapping, EnvVar("DAGSTER_ENV"))
    for rdp_mapping in RDP_MAPPINGS
]
rdp_source_assets = [
    define_rdp_src_asset(rdp_mapping, EnvVar("DAGSTER_ENV"))
    for rdp_mapping in RDP_MAPPINGS
]
dbt_assets = load_assets_from_modules([dbt])

azure_blob_resource = AzureBlobResource(
    account_url=EnvVar("AZURE_STORAGE_URL"),
    credential=EnvVar("AZURE_STORAGE_ACCESS_KEY"),
)
erp_resource = ERPResource(
    oracle_analytics_publisher_url=EnvVar("ORACLE_ANALYTICS_PUBLISHER_URL"),
    unhcr_email=EnvVar("UNHCR_EMAIL"),
    unhcr_password=EnvVar("UNHCR_PASSWORD"),
)
fusion_resource = FusionResource(
    azure_database_credential_scope=EnvVar("AZURE_DATABASE_CREDENTIAL_SCOPE"),
    odbc_driver=EnvVar("ODBC_DRIVER"),
    fusion_server=EnvVar("FUSION_SERVER"),
    fusion_database=EnvVar("FUSION_DATABASE"),
)
msrp_resource = MSRPResource(
    odbc_driver=EnvVar("ODBC_DRIVER"),
    msrp_server=EnvVar("MSRP_SERVER"),
    msrp_database=EnvVar("MSRP_DATABASE"),
    msrp_login=EnvVar("MSRP_LOGIN"),
    msrp_password=EnvVar("MSRP_PASSWORD"),
)
der_resource = DERResource(
    power_bi_credential_scope=EnvVar("POWER_BI_CREDENTIAL_SCOPE"),
    der_group_id=EnvVar("DER_GROUP_ID"),
    der_dataset_id=EnvVar("DER_DATASET_ID"),
)
rdp_resource = RDPResource(
    azure_database_credential_scope=EnvVar("AZURE_DATABASE_CREDENTIAL_SCOPE"),
    odbc_driver=EnvVar("ODBC_DRIVER"),
    rdp_server=EnvVar("RDP_SERVER"),
    rdp_database=EnvVar("RDP_DATABASE"),
)
dbt_project_dir = EnvVar("DBT_PROJECT_DIR").get_value()
dbt_resource = DbtCliResource(project_dir=dbt_project_dir)


defs = Definitions(
    assets=[
        *der_blob_assets,
        *der_source_assets,
        *erp_blob_assets,
        *erp_source_assets,
        *msrp_blob_assets,
        *msrp_source_assets,
        *rdp_blob_assets,
        *rdp_source_assets,
        *dbt_assets,
    ],
    resources={
        "azure_blob_resource": azure_blob_resource,
        "erp_resource": erp_resource,
        "fusion_resource": fusion_resource,
        "msrp_resource": msrp_resource,
        "der_resource": der_resource,
        "rdp_resource": rdp_resource,
        "dbt_resource": dbt_resource,
    },
    sensors=[
        der_timestamp_sensor,
        erp_active_timestamp_sensor,
        erp_all_timestamp_sensor,
        rdp_timestamp_sensor,
        dbt_run_status_sensor,
    ],
)
