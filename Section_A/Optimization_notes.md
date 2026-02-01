# Optimization Notes - GigaMart Pipeline

Things I learned while building this pipeline and areas that need improvement for production.

## Quick Wins (Should implement first)

1. **Partitioning** - Most important optimization
2. **Stop using SELECT *** - Biggest cost waste
3. **Set lifecycle policies on GCS** - Easy storage savings
4. **Materialized views for dashboards** - Huge performance boost

## Areas Covered

1. Storage & Table Design
2. Query Optimization
3. Ingestion Improvements
4. Cost Reduction
5. Pipeline Tuning

---

## 1. Storage & Table Design

### Partitioning (CRITICAL - not implemented yet)

Right now the fact_transactions table scans the entire table every time. This is expensive and slow.

**Solution**: Partition by transaction_date

```sql
-- Need to recreate table with partitioning
CREATE OR REPLACE TABLE `gigmart.fact_transactions`
PARTITION BY transaction_date
CLUSTER BY customer_sk, product_id
AS SELECT * FROM `gigmart.fact_transactions`;
```

Why this matters:

- Most queries filter by date (last 7 days, last month, etc.)
- Without partitioning, BigQuery scans ALL data even if you only want today
- Partitioning = only scan the partitions you need
- Can save 80-90% on query costs

Example: Query for yesterday's sales

- Without partition: Scans entire table (maybe 1 TB) = $5
- With partition: Scans only yesterday (maybe 5 GB) = $0.025

### Clustering

After partitioning, add clustering on frequently joined/filtered columns:

```sql
-- Cluster by columns we use in WHERE/JOIN
CLUSTER BY customer_sk, product_id
```

Clustering helps when you:

- Filter by customer_sk often
- Join with dim_customers frequently
- Need product-level analysis

Note: Can only cluster up to 4 columns. Choose the most used ones.

### GCS Lifecycle Policies (Easy cost savings)

Raw data files sit in Cloud Storage forever. Most are never accessed again after loading.

**Setup lifecycle rules**:

- After 30 days: Move to Nearline storage (cheaper)
- After 90 days: Move to Coldline
- After 1 year: Archive
- Keep for 7 years for compliance, then delete

```bash
# Apply lifecycle via gsutil
gsutil lifecycle set lifecycle.json gs://bucket-name
```

Storage costs:

- Standard: $0.02/GB/month (what we use now)
- Nearline: $0.01/GB/month (half price)
- Coldline: $0.004/GB/month
- Archive: $0.0012/GB/month

For historical data that's rarely accessed, can save 60-80%.

### Staging Table Cleanup

Staging tables (stg_*) don't need to be kept forever. Once data is in the warehouse, can delete staging.

```sql
-- Auto-delete staging data after 7 days
ALTER TABLE `gigmart.stg_pos_transactions` 
SET OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY));
```

This prevents staging tables from growing indefinitely.

### File Format (Future consideration)

Currently using CSV and JSON. These are easy to work with but not optimal.

**Better options**:

- Parquet: Columnar format, better for analytics, smaller size
- Avro: Good for streaming, better compression

**Why not implemented**: CSV/JSON work fine for current volume, but if data grows 10x+, should consider Parquet.

Example conversion:

```python
# Export BigQuery table to Parquet (smaller, faster to query)
job_config = bigquery.ExtractJobConfig()
job_config.destination_format = bigquery.DestinationFormat.PARQUET
job_config.compression = bigquery.Compression.SNAPPY
```

Rough comparison:

- CSV: 100 GB
- Parquet: 25 GB (4x smaller, faster queries)

---

## 2. Query Optimization

### Stop using SELECT *

Biggest waste of money. BigQuery charges per data scanned, and columnar storage means each column is stored separately.

Bad:

```sql
SELECT * FROM fact_transactions 
WHERE transaction_date = '2026-01-15';
```

This scans ALL columns even if you only display 3.

Good:

```sql
SELECT transaction_id, total_amount, customer_sk 
FROM fact_transactions 
WHERE transaction_date = '2026-01-15';
```

If you have 20 columns but only need 3, you save 85% of scan costs.

Real example: Dashboard that shows daily revenue only needs transaction_date and total_amount, not all customer/product details.

### JOIN Optimization

JOINs can be slow if not done right.

**Key rules**:

1. **Large table first (LEFT side)**

   ```sql
   -- Put fact table first, dimension second
   FROM fact_transactions f
   LEFT JOIN dim_customers c ON f.customer_sk = c.customer_sk
   ```
2. **Use INNER JOIN when you don't need all rows**

   - INNER JOIN filters early (faster)
   - LEFT JOIN keeps all rows then filters (slower)
3. **Cluster on JOIN columns** (customer_sk, product_id)
4. **Pre-filter before joining**

   ```sql
   -- Filter fact table first, then join
   WITH recent_txn AS (
       SELECT * FROM fact_transactions 
       WHERE transaction_date >= '2026-01-01'
   )
   SELECT * FROM recent_txn
   JOIN dim_customers USING (customer_sk);
   ```

### Use CTEs for Readability

Not really a performance thing, but makes queries easier to understand and debug.

Good (CTE):

```sql
WITH high_value AS (
    SELECT customer_sk, SUM(total_amount) as total
    FROM fact_transactions
    GROUP BY customer_sk
    HAVING SUM(total_amount) > 1000
)
SELECT c.name, h.total
FROM high_value h
JOIN dim_customers c USING (customer_sk);
```

vs nested subqueries (messy, hard to debug)

BigQuery optimizes both the same way, but CTEs are way easier to work with.

---

## 3. Ingestion Improvements

### Load Multiple Files at Once

Currently loading files one by one. Can load all files matching a pattern in single job.

```python
# Instead of looping through files, use wildcard
uri = 'gs://bucket/crm/crm_customers_*.csv'
load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
```

Benefits:

- Faster (parallel processing)
- One API call instead of many
- Either all succeed or all fail (atomic)

### Parallel File Uploads

For large file uploads, use gsutil with -m flag for parallel:

Much faster than uploading files one by one.

### Streaming (Future Enhancement)

Current setup is batch (hourly loads). For true real-time:

- POS → Pub/Sub → Dataflow → BigQuery streaming insert

Not implemented yet because batch is good enough for now (hourly is fine for dashboards).

Streaming adds complexity and cost. Only worth it if business needs sub-minute latency.

---

## 4. Cost Control

### Flat-Rate Pricing (Consider if usage grows)

Currently using on-demand pricing: $5 per TB scanned

If we scan >10 TB/day consistently, flat-rate (slots) might be cheaper.

Example:

- On-demand: 10 TB/day = $50/day = $1500/month
- Flex slots (100 slots for 2 hours): ~$8/day = $240/month

Worth considering if ETL becomes predictable and heavy.

For now, on-demand is fine since we're not scanning that much data yet.

### Find Expensive Queries

Check INFORMATION_SCHEMA to see which queries cost the most:

```sql
SELECT
    user_email,
    query,
    total_bytes_billed / POWER(10, 12) as TB_scanned,
    total_bytes_billed / POWER(10, 12) * 5 as cost_usd
FROM `region-asia-east1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY total_bytes_billed DESC
LIMIT 20;
```

This shows top 20 most expensive queries. Good for finding accidental full table scans.

### Avoid Functions on Partition Column

This breaks partition pruning:

```sql
-- BAD - scans entire table
WHERE EXTRACT(MONTH FROM transaction_date) = 1

-- GOOD - uses partition
WHERE transaction_date BETWEEN '2026-01-01' AND '2026-01-31'
```

---

## 5. Pipeline Performance

### Incremental Loads (Not fully implemented)

Currently rebuilding entire fact table each time. Wasteful for large tables.

Better approach: Only process new data

```sql
-- Only process yesterday's data
INSERT INTO fact_transactions
SELECT * FROM staging
WHERE transaction_date = CURRENT_DATE() - 1
```

Even better: Use MERGE for updates

```sql
MERGE fact_transactions T
USING staging S
ON T.transaction_id = S.transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

This handles both inserts and updates (late-arriving data).

Not fully implemented yet - currently doing full reloads, but important for production.

### Run Independent Tasks in Parallel

Airflow DAG has some tasks that don't depend on each other. Can run in parallel:

```python
# Dimensions can build in parallel (don't depend on each other)
[task_dim_customers, task_dim_product] >> task_build_facts
```

Currently some tasks run one after another when they could run together. Can speed up DAG by 2-3x.

---

## 6. Monitoring Things to Watch

### Query Performance

Use EXPLAIN to see execution plan for slow queries:

```sql
EXPLAIN SELECT COUNT(*) FROM fact_transactions 
WHERE transaction_date = '2026-01-01';
```

Look for:

- Full table scans (bad - should use partitions)
- Large shuffles (optimize joins)

---

## 7. Priority Order for Implementation

If I had to implement these in order of impact:


 **Priority (Do first)**:

1. ✅ Partitioning on fact_transactions - biggest cost saver
2. ✅ Stop using SELECT * - easy fix, big impact
3. Materialized views for common dashboard queries
4. GCS lifecycle policies - easy setup

**Later Priority**:
5. Incremental loads instead of full reloads
6. Clustering on frequently joined columns
7. Run parallel tasks in Airflow DAG
8. Query cost monitoring and alerts


**Estimated Impact**:

- Query costs: Could cut by 60-70% with partitioning + materialized views
- Performance: 5-10x faster queries with partitioning + clustering
- Pipeline speed: 2-3x faster with parallel tasks + incremental loads
