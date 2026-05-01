create or replace table analytics.bronze.raw_dvd_film (
    _ingest_ts timestamp,
    _source_file string,
    _year int,
    _month int,
    _day int,
    _raw variant,

    film_id int,
    title string,
    description string,
    release_year int,
    language_id int,
    rental_duration int,
    rental_rate number(4,2),
    length int,
    replacement_cost number(5,2),
    rating string,
    special_features string,
    fulltext string,
    last_update timestamp

)
cluster by (_year, _month, _day);
