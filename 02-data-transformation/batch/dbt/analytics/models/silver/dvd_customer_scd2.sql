{{ config(
    materialized='incremental',
    unique_key=['customer_id','valid_from']
) }}

-- ==================================================
-- 1. Identify customers that changed since last run
-- ==================================================

with changed_customers as (

    select distinct customer_id
    from {{ source('dvd_rental','raw_dvd_customer') }}

    {% if is_incremental() %}
    where last_update >= (
        select coalesce(max(valid_from),'1900-01-01')
        from {{ this }}
    )
    {% endif %}

),

-- ==================================================
-- 2. Pull full bronze history with deterministic ordering
-- ==================================================

numbered as (

    select
        r.customer_id,
        r.store_id,
        r.first_name,
        r.last_name,
        r.email,
        r.address_id,
        r.active,
        r.create_date,
        r.last_update,
        r._ingest_ts,

        -- business change fingerprint
        md5(concat_ws('||',
            cast(r.store_id as string),
            coalesce(r.first_name,''),
            coalesce(r.last_name,''),
            coalesce(r.email,''),
            cast(r.address_id as string),
            cast(r.active as string)
        )) as record_hash,

        lag(
            md5(concat_ws('||',
                cast(r.store_id as string),
                coalesce(r.first_name,''),
                coalesce(r.last_name,''),
                coalesce(r.email,''),
                cast(r.address_id as string),
                cast(r.active as string)
            ))
        ) over (
            partition by r.customer_id
            order by r.last_update, r._ingest_ts
        ) as prev_hash

    from {{ source('dvd_rental','raw_dvd_customer') }} r
    join changed_customers c
        on r.customer_id = c.customer_id

),

-- ==================================================
-- 3. Keep only real business changes
-- ==================================================

filtered as (

    select
        customer_id,
        store_id,
        first_name,
        last_name,
        email,
        address_id,
        active,
        create_date,
        last_update,
        _ingest_ts,
        last_update as valid_from

    from numbered
    where prev_hash is null
       or record_hash <> prev_hash

),

-- ==================================================
-- 4. Build deterministic validity windows
-- ==================================================

final_windows as (

    select
        *,
        lead(valid_from) over (
            partition by customer_id
            order by valid_from, _ingest_ts
        ) as valid_to

    from filtered

),

-- ==================================================
-- 5. Final SCD2 output
-- ==================================================

scd2 as (

    select
        customer_id,
        store_id,
        first_name,
        last_name,
        email,
        address_id,
        active,
        create_date,
        cast(last_update as timestamp) as last_update,
        valid_from,
        valid_to,

        case
            when valid_to is null then true
            else false
        end as is_current

    from final_windows

)

select * from scd2
