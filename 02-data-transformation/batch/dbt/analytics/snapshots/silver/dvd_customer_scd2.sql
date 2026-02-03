{% snapshot dvd_customer_scd2 %}

{{
  config(
    target_schema='silver',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='last_update'
  )
}}

with ranked as (
select
    customer_id,
    first_name,
    last_name,
    create_date,
    active,
    address_id,
    email,
    store_id,
    last_update,
    row_number() over (
        partition by customer_id 
        order by last_update desc, _airbyte_extracted_at desc
        ) as rn
from {{ source('dvd_rental', 'raw_dvd_customer') }}
)
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
from ranked
where rn = 1

{% endsnapshot %}