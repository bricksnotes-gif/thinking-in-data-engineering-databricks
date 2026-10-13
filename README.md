# Thinking in Data Engineering with Databricks

> Practice-first data engineering labs and notebooks, built for Databricks Free Edition.
> Official companion repository for [BricksNotes](https://bricksnotes.com) by UnlockTheNXT Learning Hub.

[![Databricks Free Edition](https://img.shields.io/badge/Databricks-Free%20Edition%20Ready-orange?style=flat-square&logo=databricks)](https://bricksnotes.com)
[![May 2026 Blueprint](https://img.shields.io/badge/Certification-May%202026%20Blueprint-blue?style=flat-square)](https://bricksnotes.com/practice/databricks-data-engineer-associate)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)

This repository contains runnable PySpark, Delta Lake, and SQL notebooks designed to help you understand how distributed data systems behave. Every lab runs on the **Databricks Free Edition** without paid cloud infrastructure.

More than 100,000 data professionals worldwide use BricksNotes to learn Databricks, and over 25,000 have completed the free Data Engineer Associate practice exam. This repository is the hands-on half of that learning loop.

---

## How This Repository Works

Each notebook is one half of a lesson. The other half lives on BricksNotes:

1. **Read** the companion lesson on BricksNotes to build intuition with diagrams.
2. **Run** the notebook here to observe the system behavior yourself.
3. **Test** your understanding with the free practice exams, no sign-up needed.

You can start anywhere. Each notebook is self-contained.

---

## What is Inside

```text
├── notebooks/                     # Databricks-ready Python notebooks (.py)
│   ├── 01_pyspark_fundamentals.py
│   ├── 02_delta_lake_acid_and_time_travel.py
│   ├── 03_medallion_architecture_pipeline.py
│   ├── 04_liquid_clustering_vs_partitioning.py
│   ├── 05_small_file_problem_lab.py
│   ├── 06_structured_streaming_available_now.py
│   └── 07_lakeflow_declarative_pipeline.py
├── datasets/                      # 24 real-world practice CSV datasets
├── interview-prep/                # Practical SQL and PySpark cheat sheets
│   ├── top_sql_interview_patterns.sql
│   ├── pyspark_interview_cheat_sheet.md
│   └── production_debugging_guide.md
└── certification/                 # May 2026 Associate & Professional study maps
    └── databricks_associate_study_guide_2026.md
```

---

## Quickstart: Running in Databricks Free Edition

1. Sign up for a free account at [Databricks Free Edition](https://community.cloud.databricks.com).
2. Create a standard single-node cluster (uses the default Free Edition runtime).
3. In your Databricks workspace:
   - Click **Workspace** in the sidebar.
   - Navigate to your user folder, click the dropdown arrow, and choose **Import**.
   - Choose **URL** or **File** and upload any `.py` file from the `notebooks/` directory. Databricks recognizes the `# Databricks notebook source` header automatically.
4. To upload practice datasets:
   - Go to **Catalog** -> **Create Table** or upload the files directly to your workspace volume at `/Volumes/workspace/default/book_data/`.

---

## The Learning Map

| Lab Notebook | What You Observe in Free Edition | Companion Lesson and Practice |
| :--- | :--- | :--- |
| `01_pyspark_fundamentals.py` | Lazy evaluation, Catalyst Optimizer plans, narrow vs wide shuffles | [Transformations lesson](https://bricksnotes.com/lessons/transformations) • [Associate practice exam](https://bricksnotes.com/practice/databricks-data-engineer-associate) |
| `02_delta_lake_acid_and_time_travel.py` | Transaction log (`_delta_log/`), ACID commits, time travel | [Delta Lake lesson](https://bricksnotes.com/lessons/delta-lake) • [Delta Lake tutorial](https://bricksnotes.com/delta-lake-tutorial) |
| `03_medallion_architecture_pipeline.py` | Bronze raw ingest, Silver deduplication, Gold business aggregates | [Medallion Architecture lesson](https://bricksnotes.com/lessons/medallion-architecture) • [ETL Pipelines lesson](https://bricksnotes.com/lessons/etl-pipelines) |
| `04_liquid_clustering_vs_partitioning.py` | Why Delta Lake 3.0 defaults to `CLUSTER BY` over directory partitioning | [Partitioning and Performance lesson](https://bricksnotes.com/lessons/partitioning-performance) • [Professional practice exam](https://bricksnotes.com/practice/databricks-data-engineer-professional) |
| `05_small_file_problem_lab.py` | Driver metadata bottlenecks, `OPTIMIZE`, snapshot cleanup with `VACUUM` | [Small file problem walkthrough](https://bricksnotes.com/blog/from-47-minutes-to-5-minutes-fixing-the-small-file-problem-on-delta-lake) • [Errors handbook](https://bricksnotes.com/databricks-errors) |
| `06_structured_streaming_available_now.py` | Why `Trigger.AvailableNow` replaces 24/7 clusters in production | [Streaming lesson](https://bricksnotes.com/lessons/streaming) • [Incremental Processing lesson](https://bricksnotes.com/lessons/incremental-processing) |
| `07_lakeflow_declarative_pipeline.py` | DLT expectations (`EXPECT OR DROP`), automated lineage | [ETL Pipelines lesson](https://bricksnotes.com/lessons/etl-pipelines) • [Data Quality lesson](https://bricksnotes.com/lessons/data-quality) |

---

## Free Interactive Resources on BricksNotes

- **Free Practice Exams (no sign-up required):**
  - [Databricks Certified Data Engineer Associate (May 2026 Blueprint)](https://bricksnotes.com/practice/databricks-data-engineer-associate)
  - [Databricks Certified Data Engineer Professional](https://bricksnotes.com/practice/databricks-data-engineer-professional)
  - [Databricks Certified Data Analyst Associate](https://bricksnotes.com/practice/databricks-data-analyst-associate)
- **Comprehensive Guides:**
  - [100 Databricks Interview Questions & Senior System Answers](https://bricksnotes.com/databricks-interview-guide)
  - [SQL Interview Questions for Data Engineers](https://bricksnotes.com/sql-interview-questions)
  - [PySpark Interview Questions](https://bricksnotes.com/pyspark-interview-questions)
  - [PySpark vs. Spark SQL: Speed, Examples & Decision Guide](https://bricksnotes.com/pyspark-vs-sql)
  - [Troubleshooting Databricks Errors Handbook](https://bricksnotes.com/databricks-errors)
- **The Book:**
  - [Thinking in Data Engineering with Databricks](https://bricksnotes.com) — the first chapters are free to read online.

---

## Contributing

Contributions are welcome! If you have a clean, reproducible lab that helps beginners understand data systems, please check [CONTRIBUTING.md](CONTRIBUTING.md) and open a pull request.

## License

MIT License. See [LICENSE](LICENSE) for details.
