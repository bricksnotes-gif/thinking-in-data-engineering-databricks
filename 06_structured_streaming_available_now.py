# Databricks notebook source
# MAGIC %md
# MAGIC # 06: Structured Streaming with availableNow
# MAGIC **Companion Lab for Thinking in Data Engineering with Databricks**
# MAGIC
# MAGIC Read the companion lesson with diagrams first:
# MAGIC [Structured Streaming](https://bricksnotes.com/lessons/streaming)
# MAGIC 
# MAGIC Production data engineering often prefers **Incremental Micro-Batching** over 24/7 continuous streaming.
# MAGIC Using `Trigger.AvailableNow` gives you:
# MAGIC 1. Exact checkpoint tracking and state recovery.
# MAGIC 2. Batch cluster shut-down after processing all pending data.
# MAGIC 3. Substantial cost savings on Databricks clusters.

# COMMAND ----------

from pyspark.sql import functions as F

# Create a temporary source directory with demo streaming data
dbutils.fs.mkdirs("/tmp/stream_demo/source")
dbutils.fs.mkdirs("/tmp/stream_demo/checkpoints")

sample_data = "id,value\n1,Alpha\n2,Beta\n3,Gamma"
dbutils.fs.put("/tmp/stream_demo/source/file_1.csv", sample_data, overwrite=True)

# COMMAND ----------

# Read stream incrementally
stream_df = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .schema("id INT, value STRING")
    .load("/tmp/stream_demo/source")
)

# Process all available data once, then cleanly stop
query = (
    stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/stream_demo/checkpoints")
    .trigger(availableNow=True)
    .toTable("stream_target_delta")
)

query.awaitTermination()
print("Batch complete. Cluster can now safely idle or shut down.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stream_target_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Keep Learning on BricksNotes
# MAGIC - Read the companion lessons with diagrams: [Structured Streaming](https://bricksnotes.com/lessons/streaming) and [Incremental Processing](https://bricksnotes.com/lessons/incremental-processing)
# MAGIC - Test yourself with the [free Data Engineer Associate practice exam](https://bricksnotes.com/practice/databricks-data-engineer-associate). No sign-up needed.
