from dagster import AssetSelection, RunConfig, define_asset_job, job

from ..ops import send_slack_message

MAX_CONCURENT_OPS = 4

der_job = define_asset_job(
    "der_job",
    selection=AssetSelection.key_prefixes("der"),
    tags={"target_prefix": "der"},
)
erp_active_job = define_asset_job(
    "erp_active_job",
    selection=AssetSelection.key_prefixes("erp")
    & AssetSelection.tag("status", "active"),
    tags={"target_prefix": "erp", "target_tag": "active"},
)
erp_all_job = define_asset_job(
    "erp_all_job",
    selection=AssetSelection.key_prefixes("erp"),
    tags={"target_prefix": "erp"},
)
orion_job = define_asset_job(
    "orion_job",
    selection=AssetSelection.key_prefixes("orion"),
    config=RunConfig(
        execution={"config": {"multiprocess": {"max_concurrent": MAX_CONCURENT_OPS}}}
    ),
    tags={"target_prefix": "orion"},
)
rdp_job = define_asset_job(
    "rdp_job",
    selection=AssetSelection.key_prefixes("rdp"),
    tags={"target_prefix": "rdp"},
)
sp_job = define_asset_job(
    "sp_job",
    selection=AssetSelection.key_prefixes("sp"),
    tags={"target_prefix": "sp"},
)
dbt_job = define_asset_job(
    "dbt_job",
    selection=AssetSelection.key_prefixes("dbt"),
    tags={"target_prefix": "dbt"},
)


@job
def send_slack_message_job():
    send_slack_message()
