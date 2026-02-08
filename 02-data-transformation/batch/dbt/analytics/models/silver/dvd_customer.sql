{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    address_id,
    active,
    store_id,
    create_date,
    last_update
from {{ ref('dvd_customer_scd2') }}
where dbt_valid_to is null