select
    address_id,
    address,
    phone,
    district,
    postal_code,
    city_id,
    last_update
from {{ source('dvd_rental', 'raw_dvd_address') }}
