create or replace table analytics.bronze.raw_dvd_city (
    _ingest_ts timestamp,
    _source_file string,
    _year int,
    _month int,
    _day int,
    _raw variant,

    city_id int,
    city string,
    country_id int,
    last_update timestamp

)
cluster by (_year, _month, _day);
