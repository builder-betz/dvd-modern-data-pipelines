with ranked as (
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
        last_update,
        row_number() over (
            partition by film_id
            order by last_update desc, _airbyte_extracted_at desc
        ) as rn
    from {{ source('dvd_rental', 'raw_dvd_film') }}
)
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
    cast(last_update as timestamp) as last_update
from ranked
where rn = 1
