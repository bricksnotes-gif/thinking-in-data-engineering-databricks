# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Reproduce and Fix the Small File Problem
# MAGIC
# MAGIC **Goal.** See, with your own eyes, how a streaming Delta table can quietly
# MAGIC grow into hundreds of tiny Parquet files, and how `OPTIMIZE` (and `ZORDER`)
# MAGIC fix it.
# MAGIC
# MAGIC **Runs on.** Databricks Free Edition. Serverless compute is fine.
# MAGIC No external cloud storage needed. Everything lives in a Unity Catalog Volume.
# MAGIC
# MAGIC **What you will do.**
# MAGIC 1. Create a tiny source of events.
# MAGIC 2. Stream them into a Delta table with a very short trigger, on purpose.
# MAGIC 3. Inspect the file count and run a baseline query. It will feel slow.
# MAGIC 4. Run `OPTIMIZE` (and `ZORDER`) to compact the layout.
# MAGIC 5. Re-run the same query and compare.
# MAGIC
# MAGIC **Companion reading.**
# MAGIC `bricksnotes.com/blog/from-47-minutes-to-5-minutes-fixing-the-small-file-problem-on-delta-lake`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup
# MAGIC
# MAGIC We will use the standard BricksNotes location:
# MAGIC `/Volumes/workspace/default/book_data/small_file_lab/`
# MAGIC
# MAGIC Change `CATALOG`, `SCHEMA`, or `VOLUME` if your workspace uses different names.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "book_data"

BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/small_file_lab"
SOURCE_PATH = f"{BASE_PATH}/source"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoint"

TABLE_NAME = f"{CATALOG}.{SCHEMA}.events_small_files"

# Make sure the volume exists. On Free Edition this is usually already there.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# Clean slate so the lab is reproducible.
dbutils.fs.rm(BASE_PATH, recurse=True)
spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")

print("Base path:", BASE_PATH)
print("Table:", TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate a tiny source of events
# MAGIC
# MAGIC We write 30 small JSON files into the source folder. Each file holds only
# MAGIC a handful of rows. This mimics a chatty upstream producer.

# COMMAND ----------

from pyspark.sql import functions as F
import random

random.seed(42)

num_files = 30
rows_per_file = 50
num_accounts = 20

for i in range(num_files):
    df = (
        spark.range(rows_per_file)
        .withColumn("account_id", (F.rand(seed=i) * num_accounts).cast("int"))
        .withColumn("amount", (F.rand(seed=i + 100) * 100).cast("double"))
        .withColumn("event_ts", F.current_timestamp())
        .withColumn("batch_id", F.lit(i))
    )
    df.coalesce(1).write.mode("append").json(SOURCE_PATH)

file_list = dbutils.fs.ls(SOURCE_PATH)
print(f"Source files written: {len([f for f in file_list if f.name.endswith('.json')])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Stream the events into Delta, on purpose, with a short trigger
# MAGIC
# MAGIC The point here is to **reproduce** the small file problem, not to write
# MAGIC good streaming code. We use `Trigger.AvailableNow` and let each input
# MAGIC file land as its own tiny commit. In a real pipeline you would batch.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DoubleType, TimestampType

schema = StructType([
    StructField("id", LongType()),
    StructField("account_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("event_ts", TimestampType()),
    StructField("batch_id", IntegerType()),
])

stream_df = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)   # one file per micro-batch on purpose
    .json(SOURCE_PATH)
)

query = (
    stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(TABLE_NAME)
)

query.awaitTermination()
print("Stream finished.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Look at the damage
# MAGIC
# MAGIC Count the Parquet files behind the table and time a simple filtered query.
# MAGIC On a small dataset the absolute numbers are small, but the **ratio** of
# MAGIC files to data is the part that hurts in production.

# COMMAND ----------

def count_data_files(table_name: str) -> int:
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0].asDict()
    return detail["numFiles"]

def time_query(label: str):
    import time
    # Clear caches so we measure real reads, not memory.
    spark.catalog.clearCache()
    spark.sql("CLEAR CACHE")
    t0 = time.time()
    rows = spark.sql(
        f"SELECT account_id, COUNT(*) AS n, SUM(amount) AS total "
        f"FROM {TABLE_NAME} WHERE account_id = 7 GROUP BY account_id"
    ).collect()
    elapsed = time.time() - t0
    print(f"[{label}] files={count_data_files(TABLE_NAME):>4}  "
          f"time={elapsed:6.2f}s  rows={rows}")
    return elapsed

before = time_query("BEFORE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fix the layout with OPTIMIZE and ZORDER
# MAGIC
# MAGIC `OPTIMIZE` compacts small files into fewer, larger ones.
# MAGIC `ZORDER BY (account_id)` co-locates rows that share the same account, so
# MAGIC future filters can skip whole files.

# COMMAND ----------

spark.sql(f"OPTIMIZE {TABLE_NAME} ZORDER BY (account_id)")

after = time_query("AFTER ")

print()
print(f"File count dropped, and the query went from {before:.2f}s to {after:.2f}s.")
print("Same data. Same compute. Better layout.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. What you just saw
# MAGIC
# MAGIC - The streaming write created one tiny file per micro-batch.
# MAGIC - Spark had to open and plan every one of those files for a query that
# MAGIC   only needed a fraction of the data.
# MAGIC - `OPTIMIZE` cut the file count.
# MAGIC - `ZORDER BY (account_id)` made data skipping effective for the dominant
# MAGIC   filter.
# MAGIC
# MAGIC **Takeaway.** Performance is a layout problem before it is a compute problem.
# MAGIC
# MAGIC **Where to go next.**
# MAGIC - Lesson: Partitioning and Performance — `bricksnotes.com/lessons/partitioning-performance`
# MAGIC - Lesson: Structured Streaming — `bricksnotes.com/lessons/streaming`
# MAGIC - Article: Delta Lake Best Practices (VACUUM, OPTIMIZE, Time Travel) — `bricksnotes.com/blog/delta-lake-best-practices-vacuum-optimize-time-travel-production`
# MAGIC - Article: Small File Problem and Liquid Clustering — `bricksnotes.com/blog/small-file-problem-delta-lake-liquid-clustering`
# MAGIC
# MAGIC **2026 note.** For new tables, consider Liquid Clustering instead of
# MAGIC partitioning plus Z-ORDER. It adapts as data and query patterns change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cleanup (optional)
# MAGIC
# MAGIC Uncomment to remove the lab table and files.

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
# dbutils.fs.rm(BASE_PATH, recurse=True)
# print("Cleaned up.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Keep Learning on BricksNotes
# MAGIC - Read the companion lesson with diagrams: [Debugging and Monitoring](https://bricksnotes.com/lessons/debugging-monitoring)
# MAGIC - Diagnose real issues with the [Databricks errors troubleshooting handbook](https://bricksnotes.com/databricks-errors)
# MAGIC - Performance symptoms like this appear in the [Data Engineer Professional practice exam](https://bricksnotes.com/practice/databricks-data-engineer-professional)
