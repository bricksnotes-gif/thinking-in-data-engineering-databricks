# Databricks notebook source
# MAGIC %md
# MAGIC # 07: Lakeflow Declarative Pipelines (DLT)
# MAGIC **Companion Lab for Thinking in Data Engineering with Databricks**
# MAGIC
# MAGIC Read the companion lessons with diagrams first:
# MAGIC [ETL Pipelines](https://bricksnotes.com/lessons/etl-pipelines) and
# MAGIC [Data Quality](https://bricksnotes.com/lessons/data-quality)
# MAGIC 
# MAGIC Lakeflow Declarative Pipelines let you describe WHAT your pipeline should produce, not HOW to run it.
# MAGIC Databricks handles orchestration, retries, and lineage automatically.
# MAGIC 
# MAGIC Note: This notebook uses `@dlt` decorators. Import it into a Lakeflow Declarative Pipeline in your workspace.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Bronze: raw streaming table
@dlt.table(
    comment="Raw order events appended from upstream systems"
)
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/workspace/default/book_data/orders_raw/")
    )

# COMMAND ----------

# Silver: cleaned with data quality expectations
@dlt.table(
    comment="Validated orders with typed fields and removed nulls"
)
@dlt.expect_or_drop("valid_amount", "amount IS NOT NULL AND amount > 0")
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
        .withColumn("order_ts", F.to_timestamp("event_time"))
        .withColumn("amount", F.col("amount").cast("double"))
        .dropDuplicates(["order_id"])
    )

# COMMAND ----------

# Gold: business aggregate
@dlt.table(
    comment="Daily revenue per product category for reporting"
)
def daily_revenue_gold():
    return (
        dlt.read("orders_silver")
        .groupBy(F.to_date("order_ts").alias("date"), "category")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("amount"), 2).alias("total_revenue")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### What DLT Gives You Automatically
# MAGIC 
# MAGIC 1. **Lineage Graph:** Visual DAG showing bronze -> silver -> gold dependencies.
# MAGIC 2. **Data Quality Metrics:** Track how many rows passed or failed each expectation.
# MAGIC 3. **Incremental Processing:** Only new data flows through; no full reloads.
# MAGIC 4. **Error Recovery:** Failed tasks retry with checkpoint resume.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Keep Learning on BricksNotes
# MAGIC - Read the companion lessons with diagrams: [ETL Pipelines](https://bricksnotes.com/lessons/etl-pipelines) and [Data Quality](https://bricksnotes.com/lessons/data-quality)
# MAGIC - See how Genie Code generates pipelines like this one: [Genie Code and Lakeflow jobs](https://bricksnotes.com/blog/genie-code-lakeflow-jobs-pipeline-orchestration)
# MAGIC - Test yourself with the [free Data Engineer Associate practice exam](https://bricksnotes.com/practice/databricks-data-engineer-associate). No sign-up needed.
