{{ config(materialized='view') }}

select
    customer_key,
    customer_id,
    full_name
from {{ ref('dim_customer_scd2') }}
where valid_to is null