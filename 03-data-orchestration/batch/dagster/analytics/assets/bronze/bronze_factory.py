from dagster import asset, AssetExecutionContext, AssetKey
from dagster import AssetDep, TimeWindowPartitionMapping
#from dagster import AutomationCondition
from analytics.partitions.daily import daily_partitions
import datetime as dt


def ymd(key: str):
    d = dt.date.fromisoformat(key)
    return d.year, d.month, d.day


def build_bronze_asset(config):

    table = config["name"]
    cols = config["columns"]

    typed_cols = ",\n".join(cols)

    cast_cols = ",\n".join([
        f"$1:{c}::string" for c in cols
    ])

    @asset(
        name=f"raw_dvd_{table}",
        partitions_def=daily_partitions,
        group_name="02_landing_to_wh_bronze",
        required_resource_keys={"snowflake"},
        deps=[
            AssetDep(
                AssetKey(["dvd_rental", table]),
                partition_mapping=TimeWindowPartitionMapping(),
            )
        ],
        #automation_condition=AutomationCondition.eager(),
    )
    def bronze_asset(context: AssetExecutionContext):
        context.log.info(f"Partitions def: {daily_partitions}")
        snowflake = context.resources.snowflake

        y, m, d = ymd(context.partition_key)

        stage = (
            f"@bronze_stage/landing/dvd_rental/"
            f"{table}/year={y}/month={m:02d}/day={d:02d}/"
        )

        target = f"analytics.bronze.raw_dvd_{table}"

        delete_sql = f"""
            delete from {target}
            where _year={y}
              and _month={m}
              and _day={d}
        """

        copy_sql = f"""
            copy into {target}
            (
                _ingest_ts,
                _source_file,
                _year,
                _month,
                _day,
                _raw,
                {typed_cols}
            )
            from (
                select
                    current_timestamp(),
                    metadata$filename,
                    {y},
                    {m},
                    {d},
                    object_construct(*),
                    {cast_cols}
                from {stage}
            )
            file_format = parquet_ff
            force = true
        """

        try:

            context.log.info("Deleting partition…")
            snowflake.execute_query(delete_sql)

            context.log.info(f"Stage path: {stage}")

            context.log.info("Copying data…")
            snowflake.execute_query(copy_sql)

            context.log.info("Bronze load completed.")

        except Exception as e:
            context.log.error(f"Bronze failed: {e}")
            raise

        context.log.info(
            f"{table} loaded partition {context.partition_key}"
        )

    return bronze_asset

