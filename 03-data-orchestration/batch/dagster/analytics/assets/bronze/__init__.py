from analytics.assets.bronze.bronze_factory import build_bronze_asset
from analytics.assets.bronze.bronze_tables import BRONZE_TABLES

bronze_assets = [
    build_bronze_asset(t) for t in BRONZE_TABLES
]
