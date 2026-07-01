# Airflow Workflow — From Business Requirement to Automated Job

## Overview

This document describes the **entire lifecycle** from receiving a business requirement to running an automated Airflow DAG daily/hourly.

---

## 1. Full Lifecycle

```
┌─────────────────────────────────────────────────────────────────┐
│                   BUSINESS REQUIREMENT                          │
│  "I want to know the top-selling brands every day"              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA ANALYSIS                               │
│  - What data is needed? → quantity_sold, brand_name, crawl_date │
│  - Where is the data? → Silver table: tiki.products             │
│  - What is the output? → Aggregate table, dashboard chart       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   WRITE PYTHON JOB                              │
│  src/jobs/tiki_gold.py                                          │
│  → compute_brand_performance(spark)                             │
│  → save_gold_table(..., "brand_performance", ...)               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DEFINE AIRFLOW TASK                           │
│  dags/tiki_pipeline_dag.py                                      │
│  → BashOperator(task_id="transform_gold", bash_command=...)     │
│  → load_bronze_silver_task >> transform_gold_task               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│               AIRFLOW AUTO-DISCOVERS DAG                        │
│  - File is mounted to: /opt/airflow/dags/                       │
│  - Airflow Scheduler scans every 30 seconds                     │
│  - DAG appears in UI automatically — NO restart needed          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                  SCHEDULED JOB EXECUTION                         │
│  - Every 4 hours (8h,12h,16h,20h,0h,4h):                         │
│      extract from Mock API → Kafka → consume → Bronze → Silver → Gold │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│              BUSINESS USER VIEWS DASHBOARD                       │
│  Superset (http://localhost:8088)                                │
│  → Brand Performance chart                                       │
│  → Price Trend timeline                                          │
│  → Discount Analysis heatmap                                     │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Directory Structure & Roles

```
tiki_lakehouse/
├── src/
│   ├── common/                  # Shared utilities 
│   │   ├── tiki_category.py     # Parses category tree
│   │   ├── tiki_product.py      # Extracts products from Mock API
│   │   └── utils.py             # Logger, JSON saver
│   ├── simulators/              # Data generation (Crucial for Demo)
│   │   ├── init_sqlite.py       # Initializes 180k+ products into SQLite
│   │   ├── mock_tiki_service.py # FastAPI Mock Server simulating Tiki API
│   │   └── tiki_continuous_simulator.py # Real-time event generator
│   └── jobs/                    # Executable Spark/Python jobs
│       ├── tiki_extract.py      # [Producer] Crawls Mock API → publishes to Kafka
│       ├── kafka_consumer.py    # [Consumer] Consumes Kafka → saves JSON file
│       ├── tiki_load_iceberg.py # [Load]     Bronze + Silver Iceberg tables (Spark)
│       └── tiki_gold.py         # [Transform] Gold aggregates → Iceberg + Postgres (Spark)
│
├── dags/
│   └── tiki_pipeline_dag.py     # DAG every 4h: Producer → Consumer → Bronze → Silver → Gold
│
├── docs/
│   └── airflow_workflow.md      # This document
│
├── notebooks/                   # Exploratory notebooks
├── data/                        # Local data (SQLite DB, raw JSON)
└── docker/                      # Dockerfiles and configurations
```

---

## 3. Guide to Adding a New Job (Standard Procedure)

### Step 1 — Requirement Analysis

```
Questions to answer:
✓ What is the output? (table, file, alert, ...)
✓ What is the input? (Bronze, Silver, Gold, API, ...)
✓ How often does it run? (daily, hourly, on-demand, ...)
✓ Which tasks must run first?
```

### Step 2 — Write Python Job

```python
# src/jobs/new_job.py
from common.utils import setup_logger

logger = setup_logger(__name__)

def run():
    # ... job logic
    pass

if __name__ == "__main__":
    run()
```

> **Convention**: Every job is an independent file that can be executed directly via `python job.py` without needing Airflow for testing.

### Step 3 — Manual Testing

```bash
# Test directly on host machine (requires environment setup)
PYTHONPATH=src python src/jobs/new_job.py

# Or inside the Spark container (Recommended)
docker exec tiki_spark_crawler python /home/jovyan/work/src/jobs/new_job.py
```

### Step 4 — Add Task to DAG

```python
# dags/tiki_pipeline_dag.py (or create a new DAG)
new_task = BashOperator(
    task_id="new_task_name",
    bash_command="docker exec tiki_spark_crawler python /home/jovyan/work/src/jobs/new_job.py",
    doc_md="### new_task_name\nBrief description of this task.",
)

# Connect to the chain
previous_task >> new_task >> next_task
```

### Step 5 — Airflow Auto-discovery (No restart required!)

```
After saving the file in dags/:
→ Airflow Scheduler scans within 30 seconds
→ New DAG / task appears in UI automatically
→ You can trigger it manually to test immediately
```

---

## 4. Monitoring & Troubleshooting

### Pipeline Status

| Location | URL | Information |
|---|---|---|
| Airflow UI | http://localhost:8081 | DAG runs, task status, logs |
| Spark UI | http://localhost:4040 | Spark jobs, stages, memory |
| MinIO Console | http://localhost:9001 | Iceberg files in S3 |
| Superset Dashboard | http://localhost:8088 | Business dashboards |

### When a Task Fails

```
1. Go to Airflow UI → DAG → Graph View
2. Click the red task → "Log"
3. Search for ERROR in the logs
4. Fix the code in src/jobs/
5. Airflow retries automatically (retries=2, retry_delay=5m)
   or you can manually Clear + Re-run.
```

### Checking Loaded Data

```python
# In Jupyter Notebook (http://localhost:8888)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("check").getOrCreate()

# Bronze
spark.table("local_catalog.tiki_bronze.products_raw").count()

# Silver
spark.table("local_catalog.tiki.products").count()
spark.table("local_catalog.tiki.price_history").count()

# Gold
spark.table("local_catalog.tiki_gold.brand_performance").show(10)
spark.table("local_catalog.tiki_gold.daily_summary").show()
```

---

## 5. Medallion Architecture (Overview)

```
[Mock API Simulator]
        │
        ▼ (Kafka Producer)
Kafka Topic (tiki.raw.products)
        │
        ▼ (Kafka Consumer)
RAW JSON Files (local data/)
        │
        ▼
[Bronze Layer] local_catalog.tiki_bronze.products_raw
  - Append-only, no transformations
  - Schema: Matches raw API response
  - Partitioned by crawl_date
        │
        ▼
[Silver Layer]
  ├── local_catalog.tiki.products          (SCD Type 1 — Active state)
  │     MERGE INTO: keeps the latest state
  │
  └── local_catalog.tiki.price_history     (SCD Type 4 — Full history)
        Only appends when price/discount changes
        │
        ▼
[Gold Layer] local_catalog.tiki_gold.*     (Business Aggregates)
  ├── brand_performance    → Superset: Brand Rankings chart
  ├── price_trend          → Superset: Price Timeline chart
  ├── discount_analysis    → Superset: Discount Heatmap
  ├── top_products         → Superset: Product Leaderboard
  └── daily_summary        → Superset: Homepage KPI cards
        │
        ▼
[Reporting Postgres] reporting_db
        │
        ▼
[Superset Dashboard] http://localhost:8088
```

---

## 6. Schedule Reference

| DAG | Schedule | Purpose |
|---|---|---|
| `tiki_lakehouse_pipeline` | `0 1,5,9,13,17,21 * * *` (Every 4h) | Full pipeline: Mock API → Kafka → Bronze → Silver → Gold |

> **Why 4 hours instead of daily?**: E-commerce prices fluctuate multiple times a day. Extracting every 4 hours allows `price_history` to capture more volatility, making pricing analysis much more accurate.

---

## 7. Disaster Recovery — Rebuilding Data from Bronze

### When to use this?

| Situation | Solution |
|---|---|
| Task 2/3/4/5 fails due to network/power loss | Airflow auto-retries — **do not use this script** |
| Code bug caused bad calculations for days | **Use this script** to rebuild Silver + Gold |
| Accidentally dropped Silver or Gold tables | **Use this script** to recover from Bronze |
| Need to recompute a specific date range | **Use this script** with `--from-date` and `--to-date` |

### Script: `src/jobs/tiki_disaster_recovery.py`

This script replays **each day sequentially** from Bronze, ensuring that the price change history (SCD Type 4) is perfectly reconstructed.

```
Bronze (entire raw history)
    → Filter by [from-date, to-date]
    → Drop Silver + Gold
    → Loop day-by-day (oldest → newest):
        → load_silver_history()   (SCD Type 4 — detects changes)
        → load_silver_active()    (SCD Type 1 — updates latest state)
    → Rebuild all Gold tables
```

> **Important**: Always run with `--dry-run` first to confirm the date range and avoid accidentally deleting good data.

### Example Commands

```bash
docker exec tiki_spark_crawler \
    python /home/jovyan/work/src/jobs/tiki_disaster_recovery.py --dry-run

# Full recovery (all dates in Bronze)
docker exec tiki_spark_crawler \
    python /home/jovyan/work/src/jobs/tiki_disaster_recovery.py

# Recovery for a specific date range
docker exec tiki_spark_crawler \
    python /home/jovyan/work/src/jobs/tiki_disaster_recovery.py \
    --from-date 2026-06-01 --to-date 2026-06-15
```

### Why replay day-by-day?

The SCD Type 4 logic (`price_history` table) detects price changes by **comparing Day N data with the current state of Silver**. If you dump the entire Bronze layer at once, the system loses chronological context, causing historical price changes to be inaccurate. Sequential replay solves this.
