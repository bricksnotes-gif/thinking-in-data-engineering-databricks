# PySpark Production Interview Cheat Sheet

## 1. Avoid Out-Of-Memory (OOM) Errors on Worker Nodes
- **Rule:** Never call `.collect()` or `.toPandas()` on full datasets.
- Use `.take(10)` or `.limit(100)` for local inspection.
- Filter before joining to prune partitions early.

## 2. Broadcast Join for Asymmetric Datasets
When joining a large table (100M rows) with a dimension table (< 10MB):
```python
from pyspark.sql import functions as F

joined_df = large_df.join(F.broadcast(small_dim_df), on="category_id", how="left")
```
*Why:* Avoids shuffling the 100M-row table across the network; copies the small table to all worker nodes instead.

## 3. Handle Data Skew
If one key accounts for 80% of rows, standard shuffle causes one worker to hang:
- Check distribution: `df.groupBy("key").count().sort(F.col("count").desc()).show()`
- Enable Adaptive Query Execution: `spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")`
- Or use salting (append a random suffix 1..N to skewed keys).

---

## Keep Preparing on BricksNotes

- [100 Databricks Interview Questions & Senior System Answers](https://bricksnotes.com/databricks-interview-guide) — spoken answers plus what the interviewer is really testing. The first 15 are free.
- [PySpark Interview Questions](https://bricksnotes.com/pyspark-interview-questions) — reviewed and refreshed every two weeks.
- [Troubleshooting Databricks Errors Handbook](https://bricksnotes.com/databricks-errors) — the production failures behind most interview questions.
- [Free Data Engineer Associate practice exam](https://bricksnotes.com/practice/databricks-data-engineer-associate) — no sign-up needed.
