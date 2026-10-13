# Databricks notebook source
# MAGIC %md
# MAGIC # 04: Liquid Clustering vs. Hive Partitioning
# MAGIC **Companion Lab for Thinking in Data Engineering with Databricks**
# MAGIC
# MAGIC Read the companion lesson with diagrams first:
# MAGIC [Partitioning and Performance](https://bricksnotes.com/lessons/partitioning-performance)
# MAGIC 
# MAGIC Delta Lake 3.0 introduces **Liquid Clustering**, which replaces traditional table partitioning.
# MAGIC Traditional partitioning creates directory trees like `/date=2026-10-12/country=US/`.
# MAGIC This often leads to the **small file problem** and rigid partition keys that cannot change without rewriting the entire table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_liquid;
# MAGIC 
# MAGIC -- Modern Delta Lake 3.0: Liquid Clustering
# MAGIC -- No PARTITIONED BY! Instead, cluster by the columns you frequently filter or join on.
# MAGIC CREATE TABLE sales_liquid (
# MAGIC   transaction_id BIGINT,
# MAGIC   customer_id STRING,
# MAGIC   store_id STRING,
# MAGIC   transaction_date DATE,
# MAGIC   amount DECIMAL(10,2)
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (transaction_date, store_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Advantages of Liquid Clustering
# MAGIC 
# MAGIC 1. **Flexibility:** You can change cluster keys anytime using `ALTER TABLE ... CLUSTER BY` without rewriting existing files.
# MAGIC 2. **No Data Skew Disasters:** Traditional partitioning breaks when one day has 10M rows and another has 100 rows. Liquid Clustering groups data into balanced ~1GB files automatically.
# MAGIC 3. **Incremental Optimization:** Running `OPTIMIZE sales_liquid` will only re-cluster newly added files, leaving already-clustered data untouched.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trigger clustering on newly written data
# MAGIC OPTIMIZE sales_liquid;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Keep Learning on BricksNotes
# MAGIC - Read the companion lesson with diagrams: [Partitioning and Performance](https://bricksnotes.com/lessons/partitioning-performance)
# MAGIC - Performance tuning questions appear often in the [Data Engineer Professional practice exam](https://bricksnotes.com/practice/databricks-data-engineer-professional)
# MAGIC - Prepare for interviews with the [100-question Databricks interview guide](https://bricksnotes.com/databricks-interview-guide)
