{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='rental_id'
) }}

with cutoff as (

    {% if is_incremental() %}
        select coalesce(
            max(last_update),
            cast('1900-01-01' as timestamp)
        ) as cutoff_ts
        from {{ this }}
    {% else %}
        select cast('1900-01-01' as timestamp) as cutoff_ts
    {% endif %}

),

source_data as (

    select
        rental_id,
        customer_id,
        staff_id,
        inventory_id,
        cast(rental_date as date) as rental_date,
        cast(return_date as date) as return_date,
        cast(last_update as timestamp) as last_update,
        _ingest_ts
    from {{ source('dvd_rental', 'raw_dvd_rental') }}

    {% if is_incremental() %}
        where last_update > (select cutoff_ts from cutoff)
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
        _ingest_ts,
        row_number() over (
            partition by rental_id
            order by last_update desc, _ingest_ts desc
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
    last_update
from ranked
where rn = 1
