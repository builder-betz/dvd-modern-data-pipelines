select
    film_id,
    title,
    description,
    cast(release_year as int),
    replacement_cost,
    cast(length as int),
    rating,
    rental_rate,
    cast(rental_duration as int),
    special_features,
    language_id,
    fulltext,
    last_update
from {{ source('dvd_rental', 'raw_dvd_film') }}
