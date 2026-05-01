from dagster import DailyPartitionsDefinition
import datetime as dt


daily_partitions = DailyPartitionsDefinition(
    start_date="2026-02-12",
    timezone="Australia/Sydney",
    # below schedule for testing automation
    hour_offset=4,
    minute_offset=6,
)
