from dagster import AssetSelection, define_asset_job

rdp_job = define_asset_job(
    "rdp_job",
    selection=AssetSelection.key_prefixes("rdp"),
    tags={"target_prefix": "rdp"},
)
dbt_job = define_asset_job(
    "dbt_job",
    selection=AssetSelection.key_prefixes("dbt"),
    tags={"target_prefix": "dbt"},
)
erp_active_job = define_asset_job(
    "erp_active_job",
    selection=AssetSelection.key_prefixes("erp")
    & AssetSelection.tag("status", "active"),
    tags={"target_prefix": "erp"},
)
