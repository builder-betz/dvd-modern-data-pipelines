create or replace table analytics.bronze.raw_dvd_country (
    _ingest_ts timestamp,
    _source_file string,
    _year int,
    _month int,
    _day int,
    _raw variant,

    country_id number,
    country string,
    last_update timestamp
)
cluster by (_year, _month, _day);
