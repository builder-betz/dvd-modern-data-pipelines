select
    {{ dbt_utils.generate_surrogate_key([
        "date.month_end_date",
        "rental.film_key"
    ]) }} as rental_monthly_key,
    count(*) as rental_count,
    date.month_end_date as rental_month_end_date,
    rental.film_key
from {{ ref('fact_rental') }} as rental
inner join {{ ref('dim_date') }} as date
    on rental.rental_date = date.date_day
group by
    date.month_end_date,
    rental.film_key
