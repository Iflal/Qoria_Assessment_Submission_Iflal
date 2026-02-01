/*
=============================================================================
SECTION B: ANALYTICS & REPORTING
=============================================================================

-- --------------------------------------------------------------------------
-- 1. Top 5 selling products in 2024 by total sale amount
-- --------------------------------------------------------------------------
/*
Logic:
- Filter for year 2024
- Aggegrate sum of quantity and sale_amount
- Order by sale_amount descending
*/
SELECT 
    product_id, 
    SUM(quantity) as total_quantity_sold, 
    SUM(sale_amount) as total_sale_amount
FROM `giga.transactions`
WHERE sale_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY product_id
ORDER BY total_sale_amount DESC
LIMIT 5;

-- --------------------------------------------------------------------------
-- 2. Month-over-Mont Percentage Growth in 2024
-- --------------------------------------------------------------------------
/*
Logic:
- Aggregte sales by Month.
- Use LAG() window function to get previous month's value.
- Formula: (Current - Previous) / Previous * 100
*/
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC(sale_date, MONTH) as sale_month,
        SUM(sale_amount) as monthly_total
    FROM `giga.transactions`
    WHERE sale_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY 1
)
SELECT
    CAST(sale_month AS STRING) as month,
    monthly_total as total_sale_amount,
    ROUND(
        SAFE_DIVIDE(
            monthly_total - LAG(monthly_total) OVER (ORDER BY sale_month),
            LAG(monthly_total) OVER (ORDER BY sale_month)
        ) * 100, 
        2
    ) as mom_percentage_growth
FROM monthly_sales
ORDER BY sale_month;

-- --------------------------------------------------------------------------
-- 3. High-value Gold Customers
-- --------------------------------------------------------------------------
/*
Logic:
- Join transactions and profiles.
- Filter: Gold tier, Active status, Non-returns, > $100 single transaction.
- Count distinct IDs.
*/
SELECT 
    COUNT(DISTINCT t.customer_id) as distinct_gold_customers
FROM `giga.transactions` t
JOIN `giga.customer_profiles` c ON t.customer_id = c.customer_id
WHERE c.loyalty_tier = 'Gold'
  AND c.is_active = TRUE
  AND t.is_return = FALSE
  AND t.sale_amount > 100;

-- --------------------------------------------------------------------------
-- 4. Deduplicate Transactions - late arrivals
-- --------------------------------------------------------------------------
/*
Challenge 4 from Section A (Network downtime/Duplicates).
Logic:
- Use ROW_NUMBER() partitioned by transaction_id to find the single latest record.
- Order by hidden column `ingestion_timestamp` descending.
*/
SELECT * EXCEPT(row_num)
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY ingestion_timestamp DESC
        ) as row_num
    FROM `giga.transactions`
)
WHERE row_num = 1;

-- --------------------------------------------------------------------------
-- 5. Customer Lifetime Value (CLV)
-- --------------------------------------------------------------------------
/*
Logic:
- CLV = Total Net Sales (Sales - Returns)
- Returns can be handled by checking `is_return` flag.
  (If is_return is TRUE, sale_amount is negative impact on revenue, 
   assuming sale_amount is stored as positive value in DB).
*/
SELECT 
    c.customer_id,
    c.country,
    SUM(
        CASE 
            WHEN t.is_return = TRUE THEN -1 * t.sale_amount 
            ELSE t.sale_amount 
        END
    ) as clv_amount
FROM `giga.customer_profiles` c
JOIN `giga.transactions` t ON c.customer_id = t.customer_id
GROUP BY 1, 2
ORDER BY clv_amount DESC;
