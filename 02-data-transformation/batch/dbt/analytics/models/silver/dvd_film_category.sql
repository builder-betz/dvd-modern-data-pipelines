select
    film_id,
    category_id,
    cast(max(last_update) as timestamp) as last_update
from {{ source('dvd_rental', 'raw_dvd_film_category') }}
group by film_id, category_id

