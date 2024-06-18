from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_dbt import DbtCliResource
from dagster_slack import SlackResource

from .assets import dbt
from .assets.der import define_der_blob_asset, define_der_src_asset
from .assets.der_mappings import DER_MAPPINGS
from .assets.erp import define_erp_blob_asset, define_erp_src_asset
from .assets.erp_mappings import ERP_MAPPINGS
from .assets.msrp import define_msrp_blob_asset, define_msrp_src_asset
from .assets.msrp_mappings import MSRP_MAPPINGS
from .assets.orion import define_orion_blob_asset, define_orion_src_asset
from .assets.orion_mappings import ORION_MAPPINGS
from .assets.rdp import define_rdp_blob_asset, define_rdp_src_asset
from .assets.rdp_mappings import RDP_MAPPINGS
from .assets.sp import define_sp_blob_asset, define_sp_src_asset
from .assets.sp_mappings import SP_MAPPINGS
from .jobs import send_slack_message_job, sp_job
from .resources.azblob import AzBlobResource
from .resources.azsql import AzSQLResource
from .resources.bip import BIPublisherResource
from .resources.finbi import FINBIResource
from .resources.pbi import PowerBIResource
from .resources.sp import SharepointResource
from .schedules import orion_daily_schedule
from .sensors.dbt import dbt_run_success_sensor
from .sensors.der import der_timestamp_sensor
from .sensors.erp import erp_active_timestamp_sensor, erp_all_timestamp_sensor
from .sensors.rdp import rdp_timestamp_sensor
from .sensors.slack import slack_run_failure_sensor, slack_run_success_sensor

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
orion_blob_assets = [
    define_orion_blob_asset(orion_mapping, EnvVar("DAGSTER_ENV"))
    for orion_mapping in ORION_MAPPINGS
]
orion_source_assets = [
    define_orion_src_asset(orion_mapping, EnvVar("DAGSTER_ENV"))
    for orion_mapping in ORION_MAPPINGS
]
rdp_blob_assets = [
    define_rdp_blob_asset(rdp_mapping, EnvVar("DAGSTER_ENV"))
    for rdp_mapping in RDP_MAPPINGS
]
rdp_source_assets = [
    define_rdp_src_asset(rdp_mapping, EnvVar("DAGSTER_ENV"))
    for rdp_mapping in RDP_MAPPINGS
]
sp_blob_assets = [
    define_sp_blob_asset(sp_mapping, EnvVar("DAGSTER_ENV"))
    for sp_mapping in SP_MAPPINGS
]
sp_source_assets = [
    define_sp_src_asset(sp_mapping, EnvVar("DAGSTER_ENV")) for sp_mapping in SP_MAPPINGS
]
dbt_assets = load_assets_from_modules([dbt])

blob_resource = AzBlobResource(
    blob_storage_url=EnvVar("AZURE_BLOB_STORAGE_URL"),
    storage_access_key=EnvVar("AZURE_STORAGE_ACCESS_KEY"),
)
der_resource = PowerBIResource(
    power_bi_credential_scope=EnvVar("POWER_BI_CREDENTIAL_SCOPE"),
    group_id=EnvVar("DER_GROUP_ID"),
    dataset_id=EnvVar("DER_DATASET_ID"),
)
erp_resource = BIPublisherResource(
    bi_publisher_url=EnvVar("BI_PUBLISHER_URL"),
    unhcr_email=EnvVar("UNHCR_EMAIL"),
    unhcr_password=EnvVar("UNHCR_PASSWORD"),
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
orion_resource = AzSQLResource(
    azure_database_credential_scope=EnvVar("AZURE_DATABASE_CREDENTIAL_SCOPE"),
    odbc_driver=EnvVar("ODBC_DRIVER"),
    server=EnvVar("ORION_SERVER"),
    database=EnvVar("ORION_DATABASE"),
)
rdp_resource = AzSQLResource(
    azure_database_credential_scope=EnvVar("AZURE_DATABASE_CREDENTIAL_SCOPE"),
    odbc_driver=EnvVar("ODBC_DRIVER"),
    server=EnvVar("RDP_SERVER"),
    database=EnvVar("RDP_DATABASE"),
)
sharepoint_resource = SharepointResource(
    ms_graph_credential_scope=EnvVar("MS_GRAPH_CREDENTIAL_SCOPE"),
    paru_site_id=EnvVar("PARU_SITE_ID"),
)
dbt_project_dir = EnvVar("DBT_PROJECT_DIR").get_value()
dbt_resource = DbtCliResource(project_dir=dbt_project_dir)
slack_resource = SlackResource(token=EnvVar("SLACK_BOT_TOKEN"))


defs = Definitions(
    assets=[
        *der_blob_assets,
        *der_source_assets,
        *erp_blob_assets,
        *erp_source_assets,
        *msrp_blob_assets,
        *msrp_source_assets,
        *orion_blob_assets,
        *orion_source_assets,
        *rdp_blob_assets,
        *rdp_source_assets,
        *sp_blob_assets,
        *sp_source_assets,
        *dbt_assets,
    ],
    jobs=[send_slack_message_job, sp_job],
    resources={
        "blob_resource": blob_resource,
        "der_resource": der_resource,
        "erp_resource": erp_resource,
        "fusion_resource": fusion_resource,
        "msrp_resource": msrp_resource,
        "orion_resource": orion_resource,
        "rdp_resource": rdp_resource,
        "sharepoint_resource": sharepoint_resource,
        "dbt_resource": dbt_resource,
        "slack_resource": slack_resource,
    },
    schedules=[orion_daily_schedule],
    sensors=[
        der_timestamp_sensor,
        slack_run_failure_sensor,
        slack_run_success_sensor,
        erp_active_timestamp_sensor,
        erp_all_timestamp_sensor,
        rdp_timestamp_sensor,
        dbt_run_success_sensor,
    ],
)
