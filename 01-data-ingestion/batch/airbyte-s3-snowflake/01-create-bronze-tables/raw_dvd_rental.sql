create or replace table analytics.bronze.raw_dvd_rental (
    _ingest_ts timestamp,
    _source_file string,
    _year int,
    _month int,
    _day int,
    _raw variant,

    rental_id int,
    rental_date timestamp,
    inventory_id int,
    customer_id int,
    return_date timestamp,
    staff_id int,
    last_update timestamp
)
cluster by (_year, _month, _day);
