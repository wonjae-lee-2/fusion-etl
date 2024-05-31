from pathlib import Path
from typing import Any, Mapping

from dagster import AssetExecutionContext, AssetKey, EnvVar
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets


DBT_PROJECT_DIR = Path(EnvVar("DBT_PROJECT_DIR").get_value())
DAGSTER_ENV = EnvVar("DAGSTER_ENV").get_value()

dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR)
dbt_manifest_path = (
    dbt_resource.cli(["parse", "--quiet", "-t", DAGSTER_ENV])
    .wait()
    .target_path.joinpath("manifest.json")
)


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]

        if resource_type == "source":
            source_name = dbt_resource_props["source_name"]
            return AssetKey(f"{source_name}_src__{name}").with_prefix(source_name)
        else:
            return super().get_asset_key(dbt_resource_props).with_prefix("dbt")


@dbt_assets(
    manifest=dbt_manifest_path, dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def dbt_models(context: AssetExecutionContext, dbt_resource: DbtCliResource):
    yield from dbt_resource.cli(["build", "-t", DAGSTER_ENV], context=context).stream()
