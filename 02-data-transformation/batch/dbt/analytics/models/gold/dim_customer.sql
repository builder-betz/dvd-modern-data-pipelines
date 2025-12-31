select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
    concat(coalesce(first_name,''), ' ', coalesce(last_name,'')) as full_name
--   first_name,
--   last_name,  
from {{ ref('dvd_customer') }} 
