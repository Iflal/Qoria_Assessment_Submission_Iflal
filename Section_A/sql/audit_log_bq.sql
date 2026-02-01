-- =============================================================================
-- Data Quality Audit Log Table
-- =============================================================================
-- Purpose: Track all data quality checks and their results over time
-- =============================================================================

CREATE TABLE IF NOT EXISTS `gigmart.data_quality_audit` (
    audit_id STRING,
    pipeline_run_id STRING,
    check_timestamp TIMESTAMP,
    check_name STRING,
    check_category STRING,  -- INGESTION, TRANSFORMATION, VALIDATION
    status STRING,           -- PASS, FAIL, WARNING
    records_total INT64,
    records_passed INT64,
    records_failed INT64,
    error_details STRING,
    table_name STRING
);

-- =============================================================================
-- Quarantine records Summary View
-- =============================================================================
-- Provides a quick overview of all quarantined records

CREATE OR REPLACE VIEW `gigmart.vw_quarantine_summary` AS
SELECT 
    'POS' as source,
    rejection_reason,
    COUNT(*) as record_count,
    MAX(quarantined_at) as latest_occurrence
FROM `gigmart.quarantine_pos_transactions`
GROUP BY rejection_reason

UNION ALL

SELECT 
    'ECOMMERCE' as source,
    rejection_reason,
    COUNT(*) as record_count,
    MAX(quarantined_at) as latest_occurrence
FROM `gigmart.quarantine_ecommerce_orders`
GROUP BY rejection_reason

ORDER BY record_count DESC;
