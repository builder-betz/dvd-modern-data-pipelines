from dagster import Definitions, load_assets_from_modules
from analytics.assets.airbyte import airbyte_assets, airbyte_workspace
from analytics import assets  # noqa: TID252
from analytics.assets.dbt import dbt_warehouse, dbt_warehouse_resource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=[*airbyte_assets, dbt_warehouse],
    resources={
        "airbyte": airbyte_workspace,
        "dbt_warehouse_resource": dbt_warehouse_resource
    }
)