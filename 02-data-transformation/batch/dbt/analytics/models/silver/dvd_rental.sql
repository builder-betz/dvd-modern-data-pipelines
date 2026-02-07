{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='rental_id'
) }}

with source_data as (

    select
        rental_id,
        customer_id,
        staff_id,
        inventory_id,
        cast(rental_date as date) as rental_date,
        cast(return_date as date) as return_date,
        cast(last_update as timestamp) as last_update,
        _airbyte_extracted_at
    from {{ source('dvd_rental', 'raw_dvd_rental') }}

    {% if is_incremental() %}
      -- lookback window to safely pick up late updates
      where last_update >= (
        select dateadd(day, -2, max(last_update)) from {{ this }}
      )
    {% endif %}

),

ranked as (

    select
        rental_id,
        customer_id,
        staff_id,
        inventory_id,
        rental_date,
        return_date,
        last_update,
        _airbyte_extracted_at,
        row_number() over (
            partition by rental_id
            order by last_update desc, _airbyte_extracted_at desc
        ) as rn
    from source_data

)

select
    rental_id,
    customer_id,
    staff_id,
    inventory_id,
    rental_date,
    return_date,
    cast(last_update as timestamp) as last_update
from ranked
where rn = 1
