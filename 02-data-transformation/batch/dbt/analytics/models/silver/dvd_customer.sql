{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    create_date,
    active,
    address_id,
    email,
    store_id,
    last_update
from {{ ref('dvd_customer_scd2') }}
where dbt_valid_to is null