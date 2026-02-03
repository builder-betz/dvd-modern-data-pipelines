with ranked as (
select
    payment_id,
    rental_id,
    customer_id,
    staff_id,
    amount,
    payment_date,
    row_number() over (
        partition by payment_id
        order by payment_date desc, _airbyte_extracted_at desc
    ) rn
from {{ source('dvd_rental', 'raw_dvd_payment') }}
)
select
    payment_id,
    rental_id,
    customer_id,
    staff_id,
    amount,
    payment_date
from ranked
where rn = 1

