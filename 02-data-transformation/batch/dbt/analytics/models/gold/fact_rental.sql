select
    {{ dbt_utils.generate_surrogate_key(['rental.rental_id']) }} as rental_key,
    rental.rental_id,
    {{ dbt_utils.generate_surrogate_key(['rental.customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['customer.address_id']) }} as customer_address_key,
    {{ dbt_utils.generate_surrogate_key(['inventory.film_id']) }} as film_key,
    rental.rental_date,
    rental.return_date,
    -- Duration in days
    CASE WHEN return_date IS NOT NULL
        THEN DATEDIFF(rental.return_date, rental.rental_date)
        ELSE -1
    END AS rental_duration_days,
    -- Is returned boolean
    CASE WHEN rental.return_date IS NOT NULL 
        THEN 'Y' 
        ELSE 'N' 
    END AS is_returned
from {{ ref('dvd_rental') }} rental
left join {{ ref('dvd_customer') }} customer on rental.customer_id = customer.customer_id
left join {{ ref('dvd_inventory') }} inventory on rental.inventory_id = inventory.inventory_id

