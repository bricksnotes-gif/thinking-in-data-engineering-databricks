# Production Debugging Guide for Databricks

## Symptom: "One task hangs at 99% for 20 minutes"

**Likely cause:** Data skew. One partition has 100x more rows than others.

**How to confirm:** Open the Spark UI -> Stages tab -> look at the longest-running task. Check the "Duration" and "Shuffle Read Size" columns.

**Fix:**
1. Enable Adaptive Query Execution skew handling.
2. Salt the skewed key before the join.
3. Use `OPTIMIZE ... ZORDER BY` on the frequently filtered column.

## Symptom: "Job fails with OutOfMemoryError on the driver"

**Likely cause:** Someone called `.collect()` or `.toPandas()` on a large DataFrame.

**Fix:** Replace with `.limit(100)` or `.take(10)`. Filter before collecting.

## Symptom: "Queries are slow even though the table is small"

**Likely cause:** Thousands of small files (each under 10MB) from streaming or frequent small-batch writes.

**Fix:** Run `OPTIMIZE table_name` to compact files. Run `VACUUM table_name` to clean old snapshots.

## Symptom: "MERGE INTO is extremely slow"

**Likely cause:** No Z-ORDER or clustering on the join key.

**Fix:** Add `ZORDER BY (join_key)` to your `OPTIMIZE` command, or switch to Liquid Clustering with `CLUSTER BY (join_key)`.

---

## Keep Preparing on BricksNotes

- [Troubleshooting Databricks Errors Handbook](https://bricksnotes.com/databricks-errors) — 16 common errors with causes and fixes, grouped by symptom.
- [100 Databricks Interview Questions & Senior System Answers](https://bricksnotes.com/databricks-interview-guide) — production debugging is a full module. The first 15 questions are free.
- [Data Engineer Professional practice exam](https://bricksnotes.com/practice/databricks-data-engineer-professional) — where tuning and debugging questions concentrate.
