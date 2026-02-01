-- =============================================================================
-- Cleansing Layer: POS Transactions
-- =============================================================================
-- Note: Filter out invalid/messy data before loading to fact table
-- Rejects: NULL customers, empty stores, invalid amounts, bad business logic
-- =============================================================================

CREATE OR REPLACE TABLE `gigmart.stg_pos_transactions_clean` AS
SELECT 
    transaction_id,
    store_id,
    customer_id,
    product_id,
    transaction_date,
    quantity,
    unit_price,
    amount,
    event_type,
    timestamp,
    CURRENT_TIMESTAMP() as cleansed_at
FROM `gigmart.stg_pos_transactions`
WHERE 
    -- DATA QUALITY RULES
    customer_id IS NOT NULL                              -- Reject NULL customers
    AND TRIM(COALESCE(store_id, '')) != ''              -- Reject empty store IDs
    AND SAFE_CAST(amount AS FLOAT64) IS NOT NULL        -- Reject invalid numeric values
    AND SAFE_CAST(quantity AS INT64) IS NOT NULL        
    AND SAFE_CAST(unit_price AS FLOAT64) IS NOT NULL
    
    -- BUSINESS LOGIC RULES
    AND (
        event_type = 'return' OR amount >= 0            -- Only returns can have negative amounts
    )
    AND quantity > 0                                     -- Quantity must be positive
    AND timestamp IS NOT NULL                            -- Must have valid timestamp
    AND transaction_date IS NOT NULL;

-- =============================================================================
-- Quarantine: Rejected POS Transactions
-- =============================================================================
-- Store rejected records for investigation and potential recovery
-- =============================================================================

CREATE OR REPLACE TABLE `gigmart.quarantine_pos_transactions` AS
SELECT 
    *,
    CASE 
        WHEN customer_id IS NULL THEN 'NULL_CUSTOMER_ID'
        WHEN TRIM(COALESCE(store_id, '')) = '' THEN 'EMPTY_STORE_ID'
        WHEN SAFE_CAST(amount AS FLOAT64) IS NULL THEN 'INVALID_AMOUNT'
        WHEN SAFE_CAST(quantity AS INT64) IS NULL THEN 'INVALID_QUANTITY'
        WHEN event_type != 'return' AND amount < 0 THEN 'NEGATIVE_PURCHASE_AMOUNT'
        WHEN quantity <= 0 THEN 'INVALID_QUANTITY_RANGE'
        WHEN timestamp IS NULL THEN 'NULL_TIMESTAMP'
        WHEN transaction_date IS NULL THEN 'NULL_TRANSACTION_DATE'
        ELSE 'UNKNOWN_REJECTION'
    END as rejection_reason,
    CURRENT_TIMESTAMP() as quarantined_at
FROM `gigmart.stg_pos_transactions`
WHERE 
    -- Opposite of the clean criteria
    customer_id IS NULL
    OR TRIM(COALESCE(store_id, '')) = ''
    OR SAFE_CAST(amount AS FLOAT64) IS NULL
    OR SAFE_CAST(quantity AS INT64) IS NULL
    OR SAFE_CAST(unit_price AS FLOAT64) IS NULL
    OR (event_type != 'return' AND amount < 0)
    OR quantity <= 0
    OR timestamp IS NULL
    OR transaction_date IS NULL;
