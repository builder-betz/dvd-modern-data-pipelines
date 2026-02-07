{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='rental_id',
    partition_by=['rental_year', 'rental_month']
) }}

with cutoff as (

    {% if is_incremental() %}
        select dateadd(day, -2, max(last_update)) as cutoff_ts
        from {{ this }}
    {% else %}
        select cast('1900-01-01' as timestamp) as cutoff_ts
    {% endif %}

),

fact_rental as (

    select *
    from {{ ref('fact_rental') }}
    where last_update >= (select cutoff_ts from cutoff)

)

select
    {{ dbt_utils.star(from=ref('fact_rental'), relation_alias='fact_rental', except=[
        "rental_key", "customer_key","customer_address_key","film_key"
    ]) }},
    {{ dbt_utils.star(from=ref('dim_customer'), relation_alias='dim_customer', except=["customer_key","last_update"]) }},
    {{ dbt_utils.star(from=ref('dim_address'), relation_alias='dim_address', except=["address_key"]) }},
    {{ dbt_utils.star(from=ref('dim_film'), relation_alias='dim_film', except=["film_key"]) }},
    {{ dbt_utils.star(from=ref('dim_date'), relation_alias='dim_date_rental_date', prefix='rental_date_', except=["date_day"]) }},
    {{ dbt_utils.star(from=ref('dim_date'), relation_alias='dim_date_return_date', prefix='return_date_', except=["date_day"]) }}

from fact_rental
left join {{ ref('dim_customer') }} dim_customer
  on fact_rental.customer_key = dim_customer.customer_key
left join {{ ref('dim_address') }} dim_address
  on fact_rental.customer_address_key = dim_address.address_key
left join {{ ref('dim_film') }} dim_film
  on fact_rental.film_key = dim_film.film_key
left join {{ ref('dim_date') }} dim_date_rental_date
  on fact_rental.rental_date = dim_date_rental_date.date_day
left join {{ ref('dim_date') }} dim_date_return_date
  on fact_rental.return_date = dim_date_return_date.date_day
