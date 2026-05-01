create or replace table analytics.bronze.raw_dvd_address (
    _ingest_ts timestamp,
    _source_file string,
    _year int,
    _month int,
    _day int,
    _raw variant,

    address_id int,
    address string,
    address2 string,
    district string,
    city_id int,
    postal_code string,
    phone string,
    last_update timestamp

)
cluster by (_year, _month, _day);
