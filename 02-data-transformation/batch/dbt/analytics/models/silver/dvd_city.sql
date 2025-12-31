select
    city_id,
    country_id,
    city,
    last_update
from {{ source('dvd_rental', 'raw_dvd_city') }}
