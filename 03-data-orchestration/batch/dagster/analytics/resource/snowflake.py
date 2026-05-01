from dagster_snowflake import SnowflakeResource
import dagster as dg

snowflake_resource = SnowflakeResource(
    account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
    user=dg.EnvVar("SNOWFLAKE_USER"),
    password=dg.EnvVar("SNOWFLAKE_PASSWORD"),
    warehouse=dg.EnvVar("SNOWFLAKE_WAREHOUSE"),
    database="ANALYTICS",
    schema="BRONZE",
    role=dg.EnvVar("SNOWFLAKE_ROLE"),
)
