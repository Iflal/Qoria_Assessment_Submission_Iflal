-- =============================================================================
-- Fact Table Logic: Fact_Transactions
-- =============================================================================

INSERT INTO `gigmart.fact_transactions` (
    transaction_id,
    customer_sk,
    product_id,
    transaction_date,
    quantity,
    unit_price,
    total_amount,
    source_system,
    created_at
)
WITH raw_transactions AS (
    -- 1. E-Commerce Data - cleaned tbl
    SELECT
        CAST(order_id AS STRING)    AS txn_id,
        customer_id,
        product_id,
        CAST(order_date AS DATE)    AS transaction_date,
        quantity,
        unit_price,
        total_amount,
        'ecommerce'                 AS source_system,
        CAST(timestamp AS TIMESTAMP) AS tx_timestamp,
        current_timestamp()         AS ingestion_timestamp
    FROM `gigmart.stg_ecommerce_orders_clean`

    UNION ALL

    -- 2. POS Data - cleaned tbl
    SELECT
        transaction_id              AS txn_id,
        customer_id,
        product_id,
        CAST(transaction_date AS DATE) AS transaction_date,
        quantity,
        unit_price,
        amount                      AS total_amount,
        'pos'                       AS source_system,
        CAST(timestamp AS TIMESTAMP) AS tx_timestamp,
        current_timestamp()         AS ingestion_timestamp
    FROM `gigmart.stg_pos_transactions_clean`
),

deduped_transactions AS (
    -- CHALLENGE 3: Handle Duplicate/Late Data
    -- filter for the latest record based on ingestion time
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY txn_id 
                ORDER BY ingestion_timestamp DESC
            ) as row_num
        FROM raw_transactions
    )
    WHERE row_num = 1
)
SELECT
    t.txn_id        AS transaction_id,
    
    COALESCE(dim_cust.customer_sk, -1) AS customer_sk,
    
    t.product_id AS product_id,
    
    t.transaction_date,
    t.quantity,
    t.unit_price,
    t.total_amount,
    t.source_system,
    t.tx_timestamp AS created_at
FROM deduped_transactions t
LEFT JOIN `gigmart.dim_customers` dim_cust
    ON t.customer_id = dim_cust.customer_id
    AND t.tx_timestamp >= dim_cust.valid_from
    AND (dim_cust.valid_to IS NULL OR t.tx_timestamp < dim_cust.valid_to)
LEFT JOIN `gigmart.dim_product` dim_prod
    ON t.product_id = dim_prod.product_id;