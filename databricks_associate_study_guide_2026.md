# Databricks Certified Data Engineer Associate (May 2026 Blueprint)

A calm, practical study companion. Read each domain, run the matching notebook in this repository, then test yourself on the free practice exam.

The May 2026 exam update emphasizes:

1. **Unity Catalog Fundamentals:** 3-level namespace (`catalog.schema.table`), standard volumes, data governance.
2. **Delta Lake 3.0:** Liquid Clustering (`CLUSTER BY`), deletion vectors, transaction log anatomy.
3. **Medallion Architecture:** Bronze -> Silver -> Gold data pipelines.
4. **Lakeflow Declarative Pipelines (DLT):** Expectations, incremental pipelines, schema evolution.

---

## Domain to Lab Map

| Exam Domain | Run This Notebook | Read This Lesson |
| :--- | :--- | :--- |
| Spark execution and DataFrames | `notebooks/01_pyspark_fundamentals.py` | [Transformations](https://bricksnotes.com/lessons/transformations) |
| Delta Lake and ACID | `notebooks/02_delta_lake_acid_and_time_travel.py` | [Delta Lake](https://bricksnotes.com/lessons/delta-lake) |
| Medallion Architecture | `notebooks/03_medallion_architecture_pipeline.py` | [Medallion Architecture](https://bricksnotes.com/lessons/medallion-architecture) |
| Performance and Liquid Clustering | `notebooks/04_liquid_clustering_vs_partitioning.py` | [Partitioning and Performance](https://bricksnotes.com/lessons/partitioning-performance) |
| Streaming and incremental processing | `notebooks/06_structured_streaming_available_now.py` | [Structured Streaming](https://bricksnotes.com/lessons/streaming) |
| Lakeflow Declarative Pipelines | `notebooks/07_lakeflow_declarative_pipeline.py` | [ETL Pipelines](https://bricksnotes.com/lessons/etl-pipelines) |
| Data quality and expectations | `notebooks/07_lakeflow_declarative_pipeline.py` | [Data Quality](https://bricksnotes.com/lessons/data-quality) |

---

## Free Preparation Path

1. **Practice Exam:** [bricksnotes.com/practice/databricks-data-engineer-associate](https://bricksnotes.com/practice/databricks-data-engineer-associate)
   Aligned to the May 2026 blueprint. No sign-up or account required. More than 25,000 engineers have completed it.
2. **Interview Questions:** [bricksnotes.com/databricks-interview-guide](https://bricksnotes.com/databricks-interview-guide)
   100 questions with spoken answers and senior system explanations. The first 15 are free.
3. **The Book:** [bricksnotes.com](https://bricksnotes.com)
   Thinking in Data Engineering with Databricks. The first chapters are free to read online.

## A Quiet Word of Advice

The exam rewards understanding systems, not memorizing flags. If you can explain why a shuffle happens, why a Delta commit is atomic, and why Liquid Clustering replaced partitioning, you are ready.
