select
    category_id,
    name,
    last_update
from {{ source('dvd_rental', 'raw_dvd_category') }}
