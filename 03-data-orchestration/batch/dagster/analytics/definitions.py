from dagster import Definitions

from analytics.assets.airbyte import airbyte_assets, airbyte_workspace
from analytics.assets.bronze import bronze_assets
from analytics.assets.dbt import dbt_warehouse, dbt_warehouse_resource
from analytics.resource.snowflake import snowflake_resource

from analytics.jobs.dvd_pipeline import bronze_to_dbt_job
from analytics.schedules.dvd_pipeline import daily_pipeline_schedule


defs = Definitions(
    assets=[
        *airbyte_assets,
        *bronze_assets,
        dbt_warehouse,
    ],
    jobs=[bronze_to_dbt_job],
    schedules=[daily_pipeline_schedule],
    resources={
        "airbyte": airbyte_workspace,
        "snowflake": snowflake_resource,
        "dbt_warehouse_resource": dbt_warehouse_resource,
    },
)
