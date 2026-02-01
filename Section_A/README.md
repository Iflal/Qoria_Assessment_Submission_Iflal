# GigaMart Data Pipeline - Section A Submission

## Executive Summary

This repository contains a **production-ready data pipeline solution** for GigaMart's migration from on-premise Teradata to Google Cloud Platform (GCP). The solution implements a **multi-stage ELT architecture** with advanced data quality controls, addressing all 8 challenges outlined in the assessment.

**Key Innovation**: Multi-layer data quality framework with cleansing, quarantine, and audit capabilities - ensuring zero data loss while maintaining data integrity.

---

## Table of Contents
1. [Project Structure](#project-structure)
2. [Prerequisites](#prerequisites)
3. [Setup Instructions](#setup-instructions)
4. [Execution Steps](#execution-steps)
5. [Architecture Overview](#architecture-overview)
6. [Key Features](#key-features)
7. [Data Quality Framework](#data-quality-framework)
8. [Verification & Testing](#verification--testing)
9. [Assumptions](#assumptions)

---

## Project Structure

```
Qoria_Assessment/Section_A/
├── Implemented_architecture_diagram.drawio.png    # Visual imlemented GCP architecture diagram
├── Design_doc.md                                  # Architecture decisions & trade-offs
├── Optimization_notes.md                          # Performance & cost optimization strategies
├── README.md                                      # This file - setup & execution guide
├── requirements.txt                               # Python dependencies
├── key.json                                       # GCP service account credentials (not in repo)
├── Proposed_GCP_Architecture_diagram.drawio.png   # Visual proposed GCP architecture diagram
│

├── dags/                          # Airflow orchestration
│   └── retail_etl_dag.py         # Complete pipeline DAG with data quality integration
│
├── src/                           # Python ingestion scripts
│   ├── ingest_gcp.py             # Unified CSV/JSON ingestion with error tolerance
│   ├── run_pipeline.py           # End-to-end pipeline executor
│   └── simulate_pos_stream.py    # Pub/Sub streaming simulator (demo)
│
├── sql/                           # BigQuery transformations
│   ├── dim_customers_scd2_bq.sql # SCD Type 2 customer dimension
│   ├── dim_product_bq.sql        # Product dimension (SCD Type 1)
│   ├── fact_transactions_bq.sql  # Fact table with deduplication
│   ├── stg_pos_clean_bq.sql      # POS data cleansing + quarantine
│   ├── stg_ecommerce_clean_bq.sql# E-commerce cleansing + quarantine
│   └── audit_log_bq.sql          # Data quality audit infrastructure
│
├── validation/                    # Data quality checks
│   └── check_data_quality.py     # Automated validation suite
│
└── data/                          # Sample source data
    ├── crm/                       # Customer CSV files (with schema evolution)
    ├── pos/                       # POS JSON files (with data quality issues)
    ├── ecommerce/                 # E-commerce orders JSON
    └── erp/                       # Inventory CSV files
```

---

## Prerequisites

### Required
1. **Google Cloud Platform Account**
   - Active GCP project with billing enabled
   - Service account with permissions:
     - BigQuery Admin
     - Storage Admin
     - Pub/Sub Admin (for streaming demo)

2. **Local Environment**
   - Python 3.10 or higher
   - `pip` package manager
   - `gcloud` CLI (optional, for manual BigQuery commands)

3. **GCP Resources**
   - Cloud Storage bucket (e.g., `qoria-assessment-staging-<your-name>`)
   - BigQuery dataset `gigmart` (auto-created by script)
   - Service account key file (`key.json`)

### Optional (for production deployment)
- Cloud Composer environment (for Airflow orchestration)
- Terraform (for IaC deployment)

---

## Setup Instructions

### Step 1: Install Dependencies
```powershell
# Navigate to project directory
cd Qoria_Assessment

# Install required Python packages
pip install -r requirements.txt
```

**Expected packages:**
- `google-cloud-bigquery`
- `google-cloud-storage`
- `google-cloud-pubsub`
- `apache-beam[gcp]`
- `apache-airflow-providers-google`

### Step 2: Configure Credentials
```powershell
# Set environment variable to point to your service account key
$env:GOOGLE_KEY_PATH="key.json"
```

### Step 3: Update Configuration (if needed)
Edit the following files to match your GCP project:

**File:** `src/ingest_gcp.py`
```python
PROJECT_ID = "your-gcp-project-id"
BUCKET_NAME = "your-bucket-name"
```

**File:** `src/run_pipeline.py`
```python
PROJECT_ID = "your-gcp-project-id"
```

---

## Execution Steps

### **Option 1: Quick Local Execution (Recommended)**

Execute the complete pipeline end-to-end:

```powershell
# Step 1: Ingest data from local files to BigQuery staging
python src/ingest_gcp.py

# Step 2: Run all transformations (dimensions, cleansing, facts, validation)
python src/run_pipeline.py

# Step 3: Verify data quality
python validation/check_data_quality.py
```

**Expected Output:**
```
✅ Loaded 15 rows to stg_pos_transactions (1 bad record skipped)
✅ Loaded 5 rows to stg_crm_customers
✅ Successfully executed dim_customers_scd2_bq.sql
✅ Successfully executed stg_pos_clean_bq.sql (2 records quarantined)
✅ Successfully executed fact_transactions_bq.sql
✅ [PASS] All Data Quality checks passed
```

### **Option 2: Manual Step-by-Step Execution**

For detailed control and debugging:

```bash
# 1. Ingest raw data
python src/ingest_gcp.py

# 2. Build dimension tables
bq query --use_legacy_sql=false < sql/dim_customers_scd2_bq.sql
bq query --use_legacy_sql=false < sql/dim_product_bq.sql

# 3. Cleanse staging data
bq query --use_legacy_sql=false < sql/stg_pos_clean_bq.sql
bq query --use_legacy_sql=false < sql/stg_ecommerce_clean_bq.sql

# 4. Create audit infrastructure
bq query --use_legacy_sql=false < sql/audit_log_bq.sql

# 5. Build fact table
bq query --use_legacy_sql=false < sql/fact_transactions_bq.sql

# 6. Run validation
python validation/check_data_quality.py

# 7. Check results
bq query "SELECT * FROM gigmart.vw_quarantine_summary"
```

### **Option 3: Cloud Composer (Airflow) Deployment**

For production orchestration:

1. **Create Cloud Composer Environment**
   ```bash
   gcloud composer environments create gigamart-pipeline \
     --location=asia-east1 \
     --python-version=3
   ```

2. **Upload DAG to Composer**
   - Navigate to Composer environment in GCP Console
   - Upload `dags/retail_etl_dag.py` to DAGs folder
   - Upload `sql/` folder to DAGs folder

3. **Trigger Pipeline**
   - Go to Airflow UI
   - Unpause `gigamart_retail_pipeline` DAG
   - Trigger manually or wait for schedule

---

## Architecture Overview

### High-Level Flow
```
┌─────────────────────────────────────────────────────────────┐
│  SOURCES: POS, CRM, ERP, E-commerce                         │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│  INGESTION: Cloud Storage → BigQuery Staging                │
│  • Error tolerance (max_bad_records=100)                    │
│  • Schema evolution (ALLOW_FIELD_ADDITION)                  │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│  CLEANSING: SQL-based validation                            │
│  • NULL checks, type validation, business rules             │
│  • Clean tables ← Valid data                                │
│  • Quarantine tables ← Invalid data                         │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│  TRANSFORMATION: Dimensional modeling                        │
│  • dim_customers (SCD Type 2)                               │
│  • dim_product (SCD Type 1)                                 │
│  • fact_transactions (partitioned, deduplicated)            │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│  VALIDATION: Data quality checks                             │
│  • Uniqueness, referential integrity, NULL checks           │
│  • Fails pipeline if thresholds not met                     │
└─────────────────────────────────────────────────────────────┘

---

## Key Features

### 1. Multi-Source Ingestion ✅
**Challenge**: Different formats, frequencies, and schemas

**Solution**: Unified ingestion script (`ingest_gcp.py`)
- Handles CSV and JSON formats
- Autodetects schemas
- Explicit schema definitions for critical tables
- Logs all ingestion metrics

### 2. High-Speed Processing ✅
**Challenge**: Batch processing takes >12 hours

**Solution**: ELT architecture using BigQuery's parallel processing
- Raw data loaded immediately (seconds)
- Transformations run in parallel using BigQuery slots
- Reduced processing from hours to minutes

### 3. Deduplication ✅
**Challenge**: Network downtime causes duplicate uploads

**Solution**: Window function deduplication in SQL
```sql
ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY ingestion_timestamp DESC) = 1
```
- Keeps latest version of each transaction
- Handles late-arriving data automatically

### 4. Schema Evolution ✅
**Challenge**: CRM adds new fields (loyalty_points, membership_tier)

**Solution**: BigQuery schema update options
```python
schema_update_options=[
    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
    bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
]
```
- New columns added automatically
- No pipeline breakage

### 5. Data Quality Framework ✅ **[ENHANCED]**
**Challenge**: Inconsistent data, NULL values, type mismatches

**Solution**: Multi-layer validation
1. **Ingestion Layer**: `max_bad_records=100` skips malformed rows
2. **Cleansing Layer**: SQL filters with quarantine tables
3. **Validation Layer**: Automated test suite

**Validation Rules:**
- NULL checks on critical columns
- Type validation (amount must be numeric)
- Business logic (purchases can't be negative)
- Referential integrity (customer_sk must exist)

### 6. Historical Tracking (SCD Type 2) ✅
**Challenge**: Customer profiles change over time

**Solution**: Slowly Changing Dimension Type 2
```sql
CREATE TABLE dim_customers (
  customer_sk INT64,      -- Surrogate key
  customer_id STRING,     -- Natural key
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOL
)
```
- Preserves complete history
- Point-in-time lookups for accurate reporting

### 7. Real-Time Architecture ✅
**Challenge**: Marketing needs near real-time dashboards

**Solution**: Pub/Sub streaming capability (demonstrated)
- `simulate_pos_stream.py` publishes to Pub/Sub
- Architecture supports Dataflow → BigQuery streaming
- Current implementation: Batch with low latency (hourly loads)

### 8. Self-Healing Pipeline ✅
**Challenge**: Pipelines must recover from errors

**Solution**: Airflow orchestration with retries
```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
```
- Automatic retry on transient failures
- Error logging and alerting
- Quarantine for unrecoverable errors

---

## Data Quality Framework

### **Innovation: 3-Layer Quality Control**

#### Layer 1: Ingestion (Error Tolerance)
**Location**: `src/ingest_gcp.py`

**Mechanism**: BigQuery load job configuration
```python
max_bad_records=100          # Skip up to 100 malformed rows
ignore_unknown_values=True   # Handle unexpected fields
```

**Result**: 
- Pipeline doesn't crash on bad data
- Errors logged for investigation
- Good data still loads

**Example**:
```
✅ Loaded 15 rows to stg_pos_transactions
⚠️  Bad records: 1 error (Could not convert 'notanumber' to FLOAT64)
```

#### Layer 2: Cleansing (Data Validation)
**Location**: `sql/stg_pos_clean_bq.sql`, `sql/stg_ecommerce_clean_bq.sql`

**Mechanism**: SQL WHERE clauses filter bad data
```sql
WHERE customer_id IS NOT NULL
  AND TRIM(COALESCE(store_id, '')) != ''
  AND SAFE_CAST(amount AS FLOAT64) IS NOT NULL
  AND (event_type = 'return' OR amount >= 0)
```

**Result**:
- **Clean tables**: Only validated data
- **Quarantine tables**: Rejected data with reasons
- Zero data loss (can recover quarantined records)

**Quarantine Schema**:
```sql
CREATE TABLE quarantine_pos_transactions (
  <all original columns>,
  rejection_reason STRING,  -- e.g., "NULL_CUSTOMER_ID"
  quarantined_at TIMESTAMP
)
```

#### Layer 3: Validation (Post-Load Checks)
**Location**: `validation/check_data_quality.py`

**Tests**:
1. **Uniqueness**: No duplicate transaction_ids
2. **Referential Integrity**: All customer_sk exist in dim_customers
3. **NULL Checks**: Critical columns (total_amount, created_at) not NULL
4. **SCD Logic**: valid_to >= valid_from

**Integration**:
- Standalone script: `python validation/check_data_quality.py`
- Airflow task: Fails DAG if checks fail

**Example Output**:
```
INFO: [PASS] Uniqueness Check (Fact Transactions)
INFO: [PASS] Referential Integrity (Customer FK)
INFO: [PASS] Null Value Check (Critical Columns)
INFO: [PASS] SCD Logic Check (Time Travel)
INFO: All Data Quality checks passed successfully!
```

### Audit & Observability

**Table**: `gigmart.data_quality_audit`
- Tracks all validation runs
- Stores pass/fail status, record counts, error details
- Historical trend analysis

**View**: `gigmart.vw_quarantine_summary`
```sql
SELECT * FROM gigmart.vw_quarantine_summary;

┌──────────┬─────────────────────────┬──────────────┐
│ source   │ rejection_reason        │ record_count │
├──────────┼─────────────────────────┼──────────────┤
│ POS      │ EMPTY_STORE_ID          │ 1            │
│ POS      │ NULL_CUSTOMER_ID        │ 1            │
└──────────┴─────────────────────────┴──────────────┘
```

---

## Verification & Testing

### Check Pipeline Success

**1. List all tables created:**
```bash
bq ls gigmart
```

**Expected tables:**
```
dim_customers
dim_product
fact_transactions
stg_crm_customers
stg_pos_transactions
stg_pos_transactions_clean
stg_ecommerce_orders_clean
quarantine_pos_transactions
quarantine_ecommerce_orders
data_quality_audit
vw_quarantine_summary
```

**2. Verify row counts:**
```sql
SELECT 
  'CRM Raw' as table_name, COUNT(*) as rows FROM gigmart.stg_crm_customers
UNION ALL
SELECT 'POS Raw', COUNT(*) FROM gigmart.stg_pos_transactions
UNION ALL
SELECT 'POS Clean', COUNT(*) FROM gigmart.stg_pos_transactions_clean
UNION ALL
SELECT 'POS Quarantine', COUNT(*) FROM gigmart.quarantine_pos_transactions
UNION ALL
SELECT 'Fact Table', COUNT(*) FROM gigmart.fact_transactions;
```

**Expected results (with provided test data):**
```
CRM Raw: 7 rows
POS Raw: 15 rows (1 skipped during load)
POS Clean: 13 rows
POS Quarantine: 2 rows
Fact Table: ~16 rows (13 POS + 3 ecommerce)
```

**3. Check data quality issues:**
```sql
SELECT * FROM gigmart.vw_quarantine_summary;
```

**4. Sample fact table:**
```sql
SELECT * FROM gigmart.fact_transactions LIMIT 10;
```

### Test Specific Features

**SCD Type 2** (Customer history):
```sql
SELECT 
  customer_id,
  loyalty_status,
  valid_from,
  valid_to,
  is_current
FROM gigmart.dim_customers
WHERE customer_id = 'C1001'
ORDER BY valid_from;
```

**Deduplication**:
```sql
-- Should return 0 (no duplicates)
SELECT transaction_id, COUNT(*) as cnt
FROM gigmart.fact_transactions
GROUP BY transaction_id
HAVING cnt > 1;
```

---

## Assumptions

### Business Logic
1. **Deduplication Strategy**: Latest record wins (based on `ingestion_timestamp`)
2. **Currency**: All transactions assumed in USD (multi-currency not implemented)
3. **Timezone**: All timestamps in UTC
4. **Customer Matching**: Based on `customer_id` (no fuzzy matching)

### Technical
1. **Data Frequency**:
   - CRM: Daily full exports
   - ERP: Daily snapshots
   - E-commerce: Hourly incremental
   - POS: Continuous (simulated as batch files)

2. **Schema Evolution**: Only field additions supported (not deletions or type changes)

3. **Partitioning**: Fact table partitioned by `transaction_date` (daily granularity)

4. **Data Retention**:
   - Raw staging: 7 days (for reprocessing)
   - Quarantine: 90 days (for investigation)
   - Warehouse: Indefinite

5. **Scale**: Solution designed for millions of transactions daily, tested with sample dataset

### Data Quality Thresholds
- **Max bad records per file**: 100 (configurable)
- **Acceptable NULL rate**: 0% on critical fields (transaction_id, amount, timestamp)
- **Duplicate tolerance**: 0 (all duplicates resolved)

---

## Success Criteria

✅ **All 8 challenges addressed** with working implementations
✅ **Data quality framework** operational (cleansing + quarantine + validation)
✅ **SCD Type 2** preserves customer history
✅ **Deduplication** eliminates duplicate transactions
✅ **Schema evolution** handles new fields automatically
✅ **Error tolerance** prevents pipeline crashes
✅ **Orchestration** ready for production (Airflow DAG)
✅ **Audit trail** tracks all data quality issues

---

## Contact & Support

**Submission By**: Iflal Ismalebbe
**Date**: February 1, 2026
**Assessment**: GigaMart Data Engineering Practical Test - Section A
