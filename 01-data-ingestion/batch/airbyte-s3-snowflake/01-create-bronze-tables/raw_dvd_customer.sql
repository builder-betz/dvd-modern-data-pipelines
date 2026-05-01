create or replace table analytics.bronze.raw_dvd_customer (

    -- ingestion metadata
    _ingest_ts    timestamp default current_timestamp(),
    _source_file  string,
    _year         number,
    _month        number,
    _day          number,

    -- immutable raw record
    _raw          variant not null,

    -- typed projection
    customer_id   number,
    store_id      number,
    first_name    string,
    last_name     string,
    email         string,
    address_id    number,
    activebool    boolean,
    create_date   date,
    last_update   timestamp,
    active        number

)
cluster by (_year, _month, _day);
