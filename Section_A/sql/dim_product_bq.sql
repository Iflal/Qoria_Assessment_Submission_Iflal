-- =============================================================================
-- Dimension: Dim_Product (BigQuery Version)
-- =============================================================================
-- Logic:   Extract distinct products from inventory snapshots.
-- note: this is  SCD Type 1 - Overwrite
-- =============================================================================

CREATE OR REPLACE TABLE `gigmart.dim_product` AS
SELECT
    product_id,
    ANY_VALUE(product_name) as product_name,
    ANY_VALUE(price) as current_price,
    MAX(last_updated) as latest_update,
    CURRENT_TIMESTAMP() as loaded_at
FROM `gigmart.stg_erp_inventory`
GROUP BY product_id;
