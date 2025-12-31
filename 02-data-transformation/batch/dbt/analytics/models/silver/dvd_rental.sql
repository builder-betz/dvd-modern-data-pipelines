select
    rental_id,
    customer_id,
    staff_id,
    inventory_id,
    cast(rental_date as date) as rental_date,
    cast(return_date as date) as return_date,
    last_update
from {{ source('dvd_rental', 'raw_dvd_rental') }}

