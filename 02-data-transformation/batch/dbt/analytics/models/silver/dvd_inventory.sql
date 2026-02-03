with ranked as (
select
    film_id,
    inventory_id,
    store_id,
    last_update,
    row_number() over (
        partition by inventory_id
        order by last_update desc, _airbyte_extracted_at desc
    ) as rn
from {{ source('dvd_rental', 'raw_dvd_inventory') }}
)
select
    film_id,
    inventory_id,
    store_id,
    last_update
from ranked
where rn = 1
