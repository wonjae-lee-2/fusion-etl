from dagster import AssetSelection, define_asset_job

der_job = define_asset_job(
    "der_job",
    selection=AssetSelection.key_prefixes("der"),
    tags={"target_prefix": "der"},
)
rdp_job = define_asset_job(
    "rdp_job",
    selection=AssetSelection.key_prefixes("rdp"),
    tags={"target_prefix": "rdp"},
)
cerp_active_job = define_asset_job(
    "cerp_active_job",
    selection=AssetSelection.key_prefixes("cerp")
    & AssetSelection.tag("status", "active"),
    tags={"target_prefix": "cerp", "target_tag": "active"},
)
cerp_all_job = define_asset_job(
    "cerp_all_job",
    selection=AssetSelection.key_prefixes("cerp"),
    tags={"target_prefix": "cerp"},
)
dbt_job = define_asset_job(
    "dbt_job",
    selection=AssetSelection.key_prefixes("dbt"),
    tags={"target_prefix": "dbt"},
)
