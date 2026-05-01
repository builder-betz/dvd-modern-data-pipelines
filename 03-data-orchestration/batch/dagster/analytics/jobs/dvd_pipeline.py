from dagster import define_asset_job, AssetSelection

bronze_to_dbt_job = define_asset_job(
    name="bronze_to_dbt",
    selection=AssetSelection.groups(
        "01_airbyte_src_to_landing",
        "02_landing_to_wh_bronze",
        "03_dbt_wh_transform",
    ),
)
