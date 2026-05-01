from dagster import build_schedule_from_partitioned_job
from analytics.jobs.dvd_pipeline import bronze_to_dbt_job

daily_pipeline_schedule = build_schedule_from_partitioned_job(
    job=bronze_to_dbt_job,
)
