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
dbt_job = define_asset_job(
    "dbt_job",
    selection=AssetSelection.key_prefixes("dbt"),
    tags={"target_prefix": "dbt"},
)
