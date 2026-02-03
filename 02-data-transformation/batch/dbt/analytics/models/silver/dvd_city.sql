with ranked as (
    select
    city_id,
    country_id,
    city,
    last_update,
    row_number() over (
        partition by city_id 
        order by last_update desc, _airbyte_extracted_at desc
    ) as rn
from {{ source('dvd_rental', 'raw_dvd_city') }}
)
select
    city_id,
    country_id,
    city,
    last_update
from ranked
where rn = 1

