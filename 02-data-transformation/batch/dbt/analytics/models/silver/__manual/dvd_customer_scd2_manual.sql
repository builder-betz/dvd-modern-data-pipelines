-- Steps:
-- 1. Extract latest snapshot per customer from Bronze raw (append)
-- 2. [History table] Apply SCD2 merge in Silver
-- 3. [Current view] Create current view in Silver

-- Pre-requisite: Create target SCD2 table (run once)
CREATE TABLE IF NOT EXISTS silver.dvd_customer_scd2_manual (
  customer_id INT,
  store_id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  address_id INT,
  active INT,
  create_date TIMESTAMP,
  last_update TIMESTAMP,

  record_hash STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN
)
USING DELTA;


-- Step 1: Extract latest snapshot per customer from Bronze raw (append)
CREATE OR REPLACE VIEW customer_latest AS
SELECT *
FROM (
    SELECT
        customer_id,
        store_id,
        first_name,
        last_name,
        email,
        address_id,
        active,
        create_date,
        last_update,
        _airbyte_extracted_at,

        -- tracking attributes (exclude PK)
        md5(
            concat_ws('||',
                cast(store_id as STRING),
                coalesce(first_name, ''),
                coalesce(last_name, ''),
                coalesce(email, ''),
                cast(address_id as STRING),
                cast(active as STRING)
            )
        ) as record_hash,

        row_number() over (
            partition by customer_id
            order by last_update desc, _airbyte_extracted_at desc
        ) as rn
    FROM bronze.raw_dvd_customer
) t
WHERE rn = 1;


-- Step 2a: Expire existing current record (close valid_to) when attributes change
MERGE INTO silver.dvd_customer_scd2_manual as tgt
USING customer_latest as src
ON tgt.customer_id = src.customer_id
AND tgt.is_current = TRUE
WHEN MATCHED AND src.record_hash <> tgt.record_hash THEN
  UPDATE SET
    tgt.valid_to = src.last_update,
    tgt.is_current = FALSE;


-- Step 2b: Insert new current record (new customer OR changed version)
INSERT INTO silver.dvd_customer_scd2_manual (
    customer_id,
    store_id,
    first_name,
    last_name,
    email,
    address_id,
    active,
    create_date,
    last_update,
    record_hash,
    valid_from,
    valid_to,
    is_current
)
SELECT
    src.customer_id,
    src.store_id,
    src.first_name,
    src.last_name,
    src.email,
    src.address_id,
    src.active,
    src.create_date,
    src.last_update,
    src.record_hash,
    src.last_update as valid_from,
    CAST(NULL AS TIMESTAMP) as valid_to,
    TRUE as is_current
FROM customer_latest src
LEFT JOIN silver.dvd_customer_scd2_manual tgt
    ON src.customer_id = tgt.customer_id
   AND tgt.is_current = TRUE
WHERE tgt.customer_id IS NULL
   OR src.record_hash <> tgt.record_hash;


-- Step 3: Create current view from SCD2 history table
CREATE OR REPLACE VIEW silver.dvd_customer_manual AS
SELECT *
FROM silver.dvd_customer_scd2_manual
WHERE is_current = TRUE;


-- Validate: customers with > 1 version
SELECT customer_id, COUNT(*) as versions
FROM silver.dvd_customer_scd2_manual
GROUP BY customer_id
HAVING COUNT(*) > 1;
