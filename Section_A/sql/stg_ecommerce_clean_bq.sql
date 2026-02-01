-- =============================================================================
-- Cleansing Layer: E-commerce Orders
-- =============================================================================
-- Note: Filter out invalid/messy data before loading to fact table
-- =============================================================================

CREATE OR REPLACE TABLE `gigmart.stg_ecommerce_orders_clean` AS
SELECT 
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    total_amount,
    payment_type,
    timestamp,
    CURRENT_TIMESTAMP() as cleansed_at
FROM `gigmart.stg_ecommerce_orders`
WHERE 
    -- DATA QUALITY RULES
    customer_id IS NOT NULL
    AND order_id IS NOT NULL
    AND SAFE_CAST(total_amount AS FLOAT64) IS NOT NULL
    AND SAFE_CAST(quantity AS INT64) IS NOT NULL
    
    -- BUSINESS LOGIC RULES
    AND total_amount >= 0                                 -- E-commerce orders should be positive
    AND quantity > 0
    AND timestamp IS NOT NULL;

-- =============================================================================
-- Quarantine: Rejected E-commerce Orders
-- =============================================================================

CREATE OR REPLACE TABLE `gigmart.quarantine_ecommerce_orders` AS
SELECT 
    *,
    CASE 
        WHEN customer_id IS NULL THEN 'NULL_CUSTOMER_ID'
        WHEN order_id IS NULL THEN 'NULL_ORDER_ID'
        WHEN SAFE_CAST(total_amount AS FLOAT64) IS NULL THEN 'INVALID_AMOUNT'
        WHEN total_amount < 0 THEN 'NEGATIVE_AMOUNT'
        WHEN quantity <= 0 THEN 'INVALID_QUANTITY'
        WHEN timestamp IS NULL THEN 'NULL_TIMESTAMP'
        ELSE 'UNKNOWN_REJECTION'
    END as rejection_reason,
    CURRENT_TIMESTAMP() as quarantined_at
FROM `gigmart.stg_ecommerce_orders`
WHERE 
    customer_id IS NULL
    OR order_id IS NULL
    OR SAFE_CAST(total_amount AS FLOAT64) IS NULL
    OR total_amount < 0
    OR quantity <= 0
    OR timestamp IS NULL;
