select
    {{ dbt_utils.generate_surrogate_key([
        'customer_id',
        'valid_from'
    ]) }} as customer_key,

    customer_id,
    concat(coalesce(first_name,''), ' ', coalesce(last_name,'')) as full_name,
    address_id,   
    valid_from,
    valid_to,
    is_current

from {{ ref('dvd_customer_scd2') }}
