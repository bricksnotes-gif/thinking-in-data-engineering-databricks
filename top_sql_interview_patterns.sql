-- Top SQL Interview Patterns for Databricks Data Engineers
-- Practice in Databricks Free Edition / SQL Warehouse
--
-- Companion prep on BricksNotes:
--   SQL Interview Questions: https://bricksnotes.com/sql-interview-questions
--   100-question interview guide: https://bricksnotes.com/databricks-interview-guide
--   Free Associate practice exam: https://bricksnotes.com/practice/databricks-data-engineer-associate

-- 1. Deduplication using Window Functions (ROW_NUMBER)
WITH ranked_records AS (
  SELECT
    order_id,
    customer_id,
    amount,
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY order_id 
      ORDER BY updated_at DESC
    ) AS row_num
  FROM raw_orders
)
SELECT order_id, customer_id, amount, updated_at
FROM ranked_records
WHERE row_num = 1;

-- 2. Incremental MERGE (SCD Type 1 Upsert)
MERGE INTO target_customers AS t
USING source_updates AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET 
    t.email = s.email,
    t.address = s.address,
    t.updated_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, email, address, created_at, updated_at)
  VALUES (s.customer_id, s.email, s.address, s.created_at, s.updated_at);

-- 3. Calculating Rolling Averages and Running Totals
SELECT
  account_id,
  transaction_date,
  amount,
  SUM(amount) OVER (
    PARTITION BY account_id
    ORDER BY transaction_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_balance,
  AVG(amount) OVER (
    PARTITION BY account_id
    ORDER BY transaction_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7day_avg
FROM transactions;

-- ---------------------------------------------------------------------------
-- Keep preparing on BricksNotes:
--   SQL Interview Questions (refreshed every two weeks):
--     https://bricksnotes.com/sql-interview-questions
--   100 Databricks Interview Questions & Senior System Answers:
--     https://bricksnotes.com/databricks-interview-guide
--   Free Associate practice exam (no sign-up):
--     https://bricksnotes.com/practice/databricks-data-engineer-associate
