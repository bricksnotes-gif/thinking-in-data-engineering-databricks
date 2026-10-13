# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Medallion Architecture Pipeline
# MAGIC **Companion Lab for Thinking in Data Engineering with Databricks**
# MAGIC
# MAGIC Read the companion lesson with diagrams first:
# MAGIC [Medallion Architecture: Bronze, Silver, Gold](https://bricksnotes.com/lessons/medallion-architecture)
# MAGIC 
# MAGIC The Medallion Architecture structures data quality progressively:
# MAGIC - **Bronze:** Raw data preserved in its original form with ingestion metadata.
# MAGIC - **Silver:** Filtered, cleaned, deduplicated, and conformed data.
# MAGIC - **Gold:** Business-level aggregated metrics ready for analytics and AI.

# COMMAND ----------

from pyspark.sql import functions as F

# Simulate raw incoming JSON events
raw_events = [
    '{"event_id": "e1", "user_id": "u101", "event_type": "view", "timestamp": "2026-10-12T09:15:00Z", "amount": null}',
    '{"event_id": "e2", "user_id": "u102", "event_type": "purchase", "timestamp": "2026-10-12T09:20:00Z", "amount": 49.99}',
    '{"event_id": "e2", "user_id": "u102", "event_type": "purchase", "timestamp": "2026-10-12T09:20:00Z", "amount": 49.99}', # duplicate
    '{"event_id": "e3", "user_id": "u101", "event_type": "purchase", "timestamp": "2026-10-12T09:35:00Z", "amount": 120.50}',
    '{"event_id": "e4", "user_id": "u103", "event_type": "view", "timestamp": "invalid_date", "amount": null}'
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Bronze Table (Raw & Auditable)
# MAGIC We keep the raw string payload and attach ingestion metadata (`ingested_at`, `source_file`).

# COMMAND ----------

bronze_df = (
    spark.createDataFrame([(r,) for r in raw_events], ["raw_payload"])
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_system", F.lit("event_gateway"))
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable("events_bronze")
display(spark.table("events_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Silver Table (Cleaned, Typed & Deduplicated)
# MAGIC Parse JSON schema, cast data types, filter malformed records, and remove duplicate event IDs.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

json_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("amount", DoubleType(), True)
])

parsed_df = (
    spark.table("events_bronze")
    .withColumn("data", F.from_json(F.col("raw_payload"), json_schema))
    .select("data.*", "ingested_at")
)

silver_df = (
    parsed_df
    .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp")))
    .filter(F.col("event_timestamp").isNotNull())  # remove corrupt dates
    .dropDuplicates(["event_id"])                  # deduplicate on business key
    .na.fill({"amount": 0.0})
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("events_silver")
display(spark.table("events_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Gold Table (Aggregated Business Reporting)
# MAGIC Calculate daily metrics by user.

# COMMAND ----------

gold_df = (
    spark.table("events_silver")
    .groupBy("user_id")
    .agg(
        F.count("event_id").alias("total_events"),
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.max("event_timestamp").alias("last_seen")
    )
)

gold_df.write.format("delta").mode("overwrite").saveAsTable("user_daily_summary_gold")
display(spark.table("user_daily_summary_gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Keep Learning on BricksNotes
# MAGIC - Read the companion lesson with diagrams: [Medallion Architecture](https://bricksnotes.com/lessons/medallion-architecture)
# MAGIC - See how this maps to production pipelines: [ETL Pipelines](https://bricksnotes.com/lessons/etl-pipelines)
# MAGIC - Test yourself with the [free Data Engineer Associate practice exam](https://bricksnotes.com/practice/databricks-data-engineer-associate). No sign-up needed.
