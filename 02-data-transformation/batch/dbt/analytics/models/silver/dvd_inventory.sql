select
    film_id,
    inventory_id,
    store_id,
    last_update
from {{ source('dvd_rental', 'raw_dvd_inventory') }}
