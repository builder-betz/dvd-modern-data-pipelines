select
    {{ dbt_utils.generate_surrogate_key(['customer_id','dbt_valid_from']) }} as customer_key,
    customer_id,
    concat(coalesce(first_name,''), ' ', coalesce(last_name,'')) as full_name,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to
from {{ ref('dvd_customer_scd2') }}