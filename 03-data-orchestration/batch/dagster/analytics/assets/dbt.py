import os
from pathlib import Path

from dagster_dbt import DbtCliResource, dbt_assets,  DagsterDbtTranslator
from dagster import AssetExecutionContext, AutomationCondition
from dagster import AssetKey


# configure dbt project resource
dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "..", "..","..","02-data-transformation", "batch", "dbt", "analytics").resolve()
dbt_warehouse_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

# generate manifest
dbt_manifest_path = (
    dbt_warehouse_resource.cli(
        ["--quiet", "parse"],
        target_path=Path("target"),
    )
    .wait()
    .target_path.joinpath("manifest.json")
)

class CustomDagsterDbtTranslator(DagsterDbtTranslator):

#    def get_automation_condition(self, dbt_resource_props):
#        model_name = dbt_resource_props.get("name")
#
#        if model_name == "dim_date":
#            return AutomationCondition.on_cron("* * * * *") #Schedule for purposes only
#
#        return AutomationCondition.eager()

    def get_group_name(self, dbt_resource_props):
        return "03_dbt_wh_transform"


    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props.get("resource_type")

        # Only remap dbt sources
        if resource_type == "source":
            table_name = dbt_resource_props["name"]

            # Match bronze Dagster asset key
            return AssetKey([table_name])

        return super().get_asset_key(dbt_resource_props)
    
# load manifest to produce asset defintion
@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def dbt_warehouse(
    context: AssetExecutionContext,
    dbt_warehouse_resource: DbtCliResource,
):
    yield from dbt_warehouse_resource.cli(
        ["build"],
        context=context,
    ).stream()