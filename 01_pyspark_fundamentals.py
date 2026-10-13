# Databricks notebook source
# MAGIC %md
# MAGIC # 01: PySpark Fundamentals
# MAGIC **Companion Lab for Thinking in Data Engineering with Databricks**
# MAGIC
# MAGIC Read the companion lesson with diagrams first:
# MAGIC [Transformations and the Spark Execution Engine](https://bricksnotes.com/lessons/transformations)
# MAGIC 
# MAGIC In this lab, you will observe:
# MAGIC 1. How Spark evaluates queries lazily.
# MAGIC 2. The difference between narrow and wide transformations.
# MAGIC 3. How to inspect physical execution plans with `explain()`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Sample employee dataset
data = [
    (101, "Amina", "Engineering", 125000.0, "NY"),
    (102, "Carlos", "Engineering", 140000.0, "CA"),
    (103, "Devi", "Marketing", 95000.0, "NY"),
    (104, "Elena", "Marketing", 102000.0, "TX"),
    (105, "Farhan", "Engineering", 115000.0, "TX"),
    (106, "Grace", "Finance", 130000.0, "NY"),
]

schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("department", StringType(), False),
    StructField("salary", DoubleType(), False),
    StructField("state", StringType(), False)
])

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observation 1: Lazy Evaluation
# MAGIC Defining a filter or column projection does NOT trigger computation.
# MAGIC Spark builds a logical plan and only computes when an action (like `count()` or `collect()`) is called.

# COMMAND ----------

# Transformation: Narrow dependency (no shuffle across nodes)
high_earners = df.filter(F.col("salary") >= 110000.0)

# Transformation: Narrow dependency
selected_cols = high_earners.select("name", "department", "salary")

# Action: This is what actually triggers cluster execution
print(f"Total high earners: {selected_cols.count()}")
display(selected_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observation 2: Wide Transformation (Shuffle)
# MAGIC Aggregations and joins require data with the same key to live on the same worker node.
# MAGIC This triggers a shuffle stage across network boundaries.

# COMMAND ----------

# Wide transformation: GROUP BY
dept_summary = (
    df.groupBy("department")
    .agg(
        F.count("emp_id").alias("headcount"),
        F.round(F.avg("salary"), 2).alias("avg_salary")
    )
    .sort(F.col("avg_salary").desc())
)

display(dept_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observation 3: Inspecting Execution Plans
# MAGIC Inspect the physical plan to verify whether Spark will exchange partitions across the network (`Exchange hashpartitioning`).

# COMMAND ----------

dept_summary.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Keep Learning on BricksNotes
# MAGIC - Read the companion lesson with diagrams: [Transformations and the Spark Execution Engine](https://bricksnotes.com/lessons/transformations)
# MAGIC - Test yourself with the [free Data Engineer Associate practice exam](https://bricksnotes.com/practice/databricks-data-engineer-associate). No sign-up needed.
# MAGIC - Prepare for interviews with the [100-question Databricks interview guide](https://bricksnotes.com/databricks-interview-guide)
