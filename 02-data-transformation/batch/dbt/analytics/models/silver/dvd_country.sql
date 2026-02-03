with ranked as (
select
    country_id,
    country,
    last_update,
    row_number() over (
        partition by country_id
        order by last_update desc, _airbyte_extracted_at desc
    ) as rn
from {{ source('dvd_rental', 'raw_dvd_country') }}
)

select
    country_id,
    country,
    last_update
from ranked
where rn = 1
