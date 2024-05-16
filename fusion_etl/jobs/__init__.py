from dagster import define_asset_job, AssetSelection

rdp_job = define_asset_job(
    "rdp_job",
    selection=AssetSelection.key_prefixes("rdp"),
    tags={"target": "rdp"},
)
dbt_job = define_asset_job(
    "dbt_job",
    selection=AssetSelection.key_prefixes("dbt"),
    tags={"target": "dbt"},
)
