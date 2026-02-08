with ranked as (
    select
        address_id,
        address,
        phone,
        district,
        postal_code,
        city_id,
        last_update,
        row_number() over (
            partition by address_id
            order by last_update desc, _airbyte_extracted_at desc
        ) rn
    from {{ source('dvd_rental', 'raw_dvd_address') }}
) 
select 
    address_id,
    address,
    phone,
    district,
    postal_code,
    city_id,
    cast(last_update as timestamp) as last_update
from ranked
where rn = 1 