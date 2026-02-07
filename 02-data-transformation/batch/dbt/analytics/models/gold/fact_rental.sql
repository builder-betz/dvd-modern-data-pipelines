{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='rental_id',
    **portable_partition(['rental_year','rental_month'])
) }}


with max_target as (

    {% if is_incremental() %}
    select coalesce(
        dateadd(day, -2, max(last_update)),
        cast('1900-01-01' as timestamp)
    ) as cutoff_last_update
    from {{ this }}
    {% else %}
    select cast('1900-01-01' as timestamp) as cutoff_last_update
    {% endif %}

),

rental as (

    select *
    from {{ ref('dvd_rental') }}
    where last_update >= (select cutoff_last_update from max_target)

)

select
    {{ dbt_utils.generate_surrogate_key(['rental.rental_id']) }} as rental_key,
    rental.rental_id,
    customer.customer_key,
    {{ dbt_utils.generate_surrogate_key(['customer.address_id']) }} as customer_address_key,
    {{ dbt_utils.generate_surrogate_key(['inventory.film_id']) }} as film_key,

    rental.rental_date,
    year(rental.rental_date) as rental_year,
    month(rental.rental_date) as rental_month,
    rental.return_date,

    case when rental.return_date is not null
        then {{ portable_datediff('rental.rental_date','rental.return_date') }} 
        else -1
    end as rental_duration_days,

    case when rental.return_date is not null
        then 'Y'
        else 'N'
    end as is_returned,

    rental.last_update

from rental

left join {{ ref('dim_customer_scd2') }} customer
    on rental.customer_id = customer.customer_id
    and cast(rental.rental_date as timestamp) >= customer.valid_from
    and cast(rental.rental_date as timestamp)
        < coalesce(customer.valid_to, cast('9999-01-01' as timestamp))

left join {{ ref('dvd_inventory') }} inventory
    on rental.inventory_id = inventory.inventory_id
