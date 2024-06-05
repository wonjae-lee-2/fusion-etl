from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import dbt
from .assets.cerp import define_cerp_blob_asset, define_cerp_src_asset
from .assets.cerp_mappings import CERP_MAPPINGS
from .assets.der import define_der_blob_asset, define_der_src_asset
from .assets.der_mappings import DER_MAPPINGS
from .assets.msrp import define_msrp_blob_asset, define_msrp_src_asset
from .assets.msrp_mappings import MSRP_MAPPINGS
from .assets.rdp import define_rdp_blob_asset, define_rdp_src_asset
from .assets.rdp_mappings import RDP_MAPPINGS
from .resources.azblob import AzBlobResource
from .resources.azsql import AzSQLResource
from .resources.bip import BIPublisherResource
from .resources.finbi import FINBIResource
from .resources.pbi import PowerBIResource
from .sensors.cerp import cerp_active_timestamp_sensor, cerp_all_timestamp_sensor
from .sensors.dbt import dbt_run_status_sensor
from .sensors.der import der_timestamp_sensor
from .sensors.rdp import rdp_timestamp_sensor

cerp_blob_assets = [
    define_cerp_blob_asset(cerp_mapping, EnvVar("DAGSTER_ENV"))
    for cerp_mapping in CERP_MAPPINGS
]
cerp_source_assets = [
    define_cerp_src_asset(cerp_mapping, EnvVar("DAGSTER_ENV"))
    for cerp_mapping in CERP_MAPPINGS
]
der_blob_assets = [
    define_der_blob_asset(der_mapping, EnvVar("DAGSTER_ENV"))
    for der_mapping in DER_MAPPINGS
]
der_source_assets = [
    define_der_src_asset(der_mapping, EnvVar("DAGSTER_ENV"))
    for der_mapping in DER_MAPPINGS
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

blob_resource = AzBlobResource(
    blob_storage_url=EnvVar("AZURE_BLOB_STORAGE_URL"),
    storage_access_key=EnvVar("AZURE_STORAGE_ACCESS_KEY"),
)
cerp_resource = BIPublisherResource(
    bi_publisher_url=EnvVar("BI_PUBLISHER_URL"),
    unhcr_email=EnvVar("UNHCR_EMAIL"),
    unhcr_password=EnvVar("UNHCR_PASSWORD"),
)
der_resource = PowerBIResource(
    power_bi_credential_scope=EnvVar("POWER_BI_CREDENTIAL_SCOPE"),
    der_group_id=EnvVar("DER_GROUP_ID"),
    der_dataset_id=EnvVar("DER_DATASET_ID"),
)
fusion_resource = AzSQLResource(
    azure_database_credential_scope=EnvVar("AZURE_DATABASE_CREDENTIAL_SCOPE"),
    odbc_driver=EnvVar("ODBC_DRIVER"),
    server=EnvVar("FUSION_SERVER"),
    database=EnvVar("FUSION_DATABASE"),
)
msrp_resource = FINBIResource(
    odbc_driver=EnvVar("ODBC_DRIVER"),
    msrp_server=EnvVar("MSRP_SERVER"),
    msrp_database=EnvVar("MSRP_DATABASE"),
    msrp_login=EnvVar("MSRP_LOGIN"),
    msrp_password=EnvVar("MSRP_PASSWORD"),
)
rdp_resource = AzSQLResource(
    azure_database_credential_scope=EnvVar("AZURE_DATABASE_CREDENTIAL_SCOPE"),
    odbc_driver=EnvVar("ODBC_DRIVER"),
    server=EnvVar("RDP_SERVER"),
    database=EnvVar("RDP_DATABASE"),
)
dbt_project_dir = EnvVar("DBT_PROJECT_DIR").get_value()
dbt_resource = DbtCliResource(project_dir=dbt_project_dir)


defs = Definitions(
    assets=[
        *cerp_blob_assets,
        *cerp_source_assets,
        *der_blob_assets,
        *der_source_assets,
        *msrp_blob_assets,
        *msrp_source_assets,
        *rdp_blob_assets,
        *rdp_source_assets,
        *dbt_assets,
    ],
    resources={
        "blob_resource": blob_resource,
        "cerp_resource": cerp_resource,
        "der_resource": der_resource,
        "fusion_resource": fusion_resource,
        "msrp_resource": msrp_resource,
        "rdp_resource": rdp_resource,
        "dbt_resource": dbt_resource,
    },
    sensors=[
        cerp_active_timestamp_sensor,
        cerp_all_timestamp_sensor,
        der_timestamp_sensor,
        rdp_timestamp_sensor,
        dbt_run_status_sensor,
    ],
)
