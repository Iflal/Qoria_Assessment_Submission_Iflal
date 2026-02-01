-- =============================================================================
-- SCD Type 2 Logic for Dim_Customers (BigQuery Version)
-- note: This scd type 2 helps to track historical changes in customer attributes
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. IDENTIFY CHANGES
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE customers_to_upsert AS
SELECT 
    stg.customer_id,
    stg.first_name,
    stg.last_name,
    stg.email,
    stg.loyalty_status,
    stg.city,
    stg.country,
    stg.updated_at
FROM `gigmart.stg_crm_customers` stg
LEFT JOIN `gigmart.dim_customers` dim 
    ON stg.customer_id = dim.customer_id 
    AND dim.is_current = TRUE
WHERE 
    dim.customer_id IS NULL -- Case A: New Customer
    OR (                    -- Case B: Changed Attributes
        dim.loyalty_status <> stg.loyalty_status OR
        dim.city <> stg.city OR
        dim.country <> stg.country OR
        dim.email <> stg.email OR
        dim.first_name <> stg.first_name OR
        dim.last_name <> stg.last_name
    );

-- -----------------------------------------------------------------------------
-- 2. EXPIRE OLD RECORDS (Update)
-- -----------------------------------------------------------------------------
UPDATE `gigmart.dim_customers` t
SET 
    valid_to = CAST(s.updated_at AS TIMESTAMP),
    is_current = FALSE
FROM customers_to_upsert s
WHERE t.customer_id = s.customer_id
  AND t.is_current = TRUE;

-- -----------------------------------------------------------------------------
-- 3. INSERT NEW RECORDS
-- -----------------------------------------------------------------------------
INSERT INTO `gigmart.dim_customers` (
    customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    loyalty_status,
    city,
    country,
    valid_from,
    valid_to,
    is_current
)
SELECT 
    ABS(FARM_FINGERPRINT(CONCAT(customer_id, CAST(updated_at AS STRING)))) as customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    loyalty_status,
    city,
    country,
    CAST(updated_at AS TIMESTAMP) as valid_from,
    NULL as valid_to,
    TRUE as is_current
FROM customers_to_upsert;
