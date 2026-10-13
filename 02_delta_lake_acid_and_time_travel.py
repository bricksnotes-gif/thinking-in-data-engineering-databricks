# Databricks notebook source
# MAGIC %md
# MAGIC # 02: Delta Lake 3.0 ACID & Time Travel
# MAGIC **Companion Lab for Thinking in Data Engineering with Databricks**
# MAGIC
# MAGIC Read the companion lesson with diagrams first:
# MAGIC [Delta Lake and the Transaction Log](https://bricksnotes.com/lessons/delta-lake)
# MAGIC 
# MAGIC Delta Lake pairs Parquet data files with an atomic JSON commit log (`_delta_log/`).
# MAGIC In this lab, you will observe:
# MAGIC 1. How inserts create immutable Parquet data files.
# MAGIC 2. How updates perform copy-on-write without mutating existing files.
# MAGIC 3. How to query past snapshots using time travel.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a clean demo Delta table in default schema
# MAGIC DROP TABLE IF EXISTS customer_orders_demo;
# MAGIC 
# MAGIC CREATE TABLE customer_orders_demo (
# MAGIC   order_id INT,
# MAGIC   customer STRING,
# MAGIC   amount DOUBLE,
# MAGIC   status STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 0: Initial Batch
# MAGIC INSERT INTO customer_orders_demo VALUES
# MAGIC   (1, 'Alice', 150.0, 'PLACED'),
# MAGIC   (2, 'Bob', 220.0, 'PLACED'),
# MAGIC   (3, 'Charlie', 85.0, 'PENDING');
# MAGIC 
# MAGIC SELECT * FROM customer_orders_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 1: Update an order status
# MAGIC -- Under the hood: Delta writes a new Parquet file with updated rows, and logs the change in _delta_log/000001.json
# MAGIC UPDATE customer_orders_demo
# MAGIC SET status = 'SHIPPED'
# MAGIC WHERE order_id = 1;
# MAGIC 
# MAGIC SELECT * FROM customer_orders_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 2: Insert new order
# MAGIC INSERT INTO customer_orders_demo VALUES
# MAGIC   (4, 'Diana', 310.0, 'PLACED');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspecting the Delta Transaction History
# MAGIC Delta tracks every operation, author, isolation level, and timestamp.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customer_orders_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel: Querying Previous Snapshots
# MAGIC You can query the table exactly as it existed before the update occurred.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query table at initial insertion (Version 0)
# MAGIC SELECT * FROM customer_orders_demo VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare current vs initial state
# MAGIC SELECT 
# MAGIC   curr.order_id,
# MAGIC   curr.customer,
# MAGIC   orig.status AS original_status,
# MAGIC   curr.status AS current_status
# MAGIC FROM customer_orders_demo curr
# MAGIC LEFT JOIN customer_orders_demo VERSION AS OF 0 orig
# MAGIC   ON curr.order_id = orig.order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Keep Learning on BricksNotes
# MAGIC - Read the companion lesson with diagrams: [Delta Lake and the Transaction Log](https://bricksnotes.com/lessons/delta-lake)
# MAGIC - Go deeper with the hands-on [Delta Lake 3.0 tutorial](https://bricksnotes.com/delta-lake-tutorial)
# MAGIC - Test yourself with the [free Data Engineer Associate practice exam](https://bricksnotes.com/practice/databricks-data-engineer-associate). No sign-up needed.
