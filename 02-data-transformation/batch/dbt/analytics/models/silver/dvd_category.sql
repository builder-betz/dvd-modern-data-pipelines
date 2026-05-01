with ranked as (
    select
        category_id,
        name,
        last_update,
        row_number() over (
            partition by category_id
            order by last_update desc, _ingest_ts desc
        ) as rn
    from {{ source('dvd_rental', 'raw_dvd_category') }}
)
select
    category_id,
    name,
    cast(last_update as timestamp) as last_update
from ranked
where rn = 1