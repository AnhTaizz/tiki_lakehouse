"""
tiki_pipeline_dag.py
====================
Main DAG: runs every 4 hours (ICT)
  Task 1 — extract_and_publish : crawl Tiki API → publish each product to Kafka topic
  Task 2 — consume_from_kafka  : consume Kafka → collect into raw JSON file
  Task 3 — load_bronze_silver  : Spark reads JSON → Bronze + Silver Iceberg
  Task 4 — transform_gold      : Spark computes Gold aggregates → Iceberg + Reporting Postgres

Flow: extract_and_publish >> consume_from_kafka >> load_bronze_silver >> transform_gold
"""

import os
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "anhtaizz",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 20, tzinfo=local_tz),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

SPARK_EXEC      = "/opt/conda/bin/python"
SPARK_CONTAINER = "tiki_spark_crawler"
WORK_DIR        = "/home/jovyan/work"
AIRFLOW_SRC     = "/opt/airflow/src"

with DAG(
    "tiki_beauty_lakehouse_pipeline",
    default_args=default_args,
    description=(
        "Near-real-time pipeline: crawl Tiki Beauty every 4h → Kafka → Bronze → Silver → Gold → Superset"
    ),
    schedule_interval="0 1,5,9,13,17,21 * * *",
    # every 4h: 08:00, 12:00, 16:00, 20:00, 00:00, 04:00 ICT (UTC+7)
    catchup=False,
    tags=["tiki", "lakehouse", "beauty", "kafka", "near-realtime"],
    doc_md="""
## Tiki Beauty Lakehouse — Near-Real-Time Pipeline

### Flow
```
[Every 4h ICT] Crawl Tiki API
    → Kafka Topic: tiki.raw.products  (streaming ingestion)
    → Consumer: collect products → raw JSON file
    → Bronze Iceberg (raw append)
    → Silver Iceberg (SCD Type 1 active + SCD Type 4 price history)
    → Gold Iceberg + Reporting Postgres (Superset dashboards)
```

### Adding a new DAG
1. Write a Python job in `src/jobs/`
2. Define a BashOperator task that calls it
3. Connect the task into the dependency chain with `>>`
4. Commit — Airflow auto-discovers the new DAG

### Troubleshooting
- Check logs in Airflow UI → Graph View → click task → Log
- Kafka topic: `tiki.raw.products`
- Bronze table: `local_catalog.tiki_bronze.products_raw`
- Silver tables: `local_catalog.tiki.products`, `local_catalog.tiki.price_history`
- Gold tables: `local_catalog.tiki_gold.*`
""",
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Extract & Publish — Crawl Tiki API → Kafka Producer
    # ------------------------------------------------------------------
    extract_task = BashOperator(
        task_id="extract_and_publish",
        env={
            **os.environ,
            "PYTHONPATH": AIRFLOW_SRC,
        },
        bash_command=f"python {AIRFLOW_SRC}/jobs/tiki_extract.py",
        do_xcom_push=True,
        doc_md="""
### extract_and_publish
Crawls all Beauty & Health categories from Tiki API (concurrent, 10 workers).
Then publishes each product to Kafka topic `tiki.raw.products` as a Producer.
XCom push: `crawl_date` in `YYYY-MM-DD` format for Task 2 (consumer) to use.
""",
    )

    # ------------------------------------------------------------------
    # Task 2: Consume from Kafka — collect products → raw JSON file
    # Runs inside the Airflow container (no Spark required)
    # ------------------------------------------------------------------
    consume_task = BashOperator(
        task_id="consume_from_kafka",
        env={
            **os.environ,
            "PYTHONPATH": AIRFLOW_SRC,
        },
        bash_command=f"""
            CRAWL_DATE="{{{{ ti.xcom_pull(task_ids='extract_and_publish') }}}}"
            echo "Consuming Kafka for crawl_date: $CRAWL_DATE"
            python {AIRFLOW_SRC}/jobs/kafka_consumer.py --crawl_date "$CRAWL_DATE"
        """,
        do_xcom_push=True,
        doc_md="""
### consume_from_kafka
Consumes all messages from Kafka topic `tiki.raw.products`.
Collects them into file `data/tiki_beauty_health_raw_YYYY-MM-DD.json`.
XCom push: absolute path of the raw file for Task 3 (Spark) to read.
""",
    )

    # ------------------------------------------------------------------
    # Task 3: Load Bronze + Silver — Spark job on Spark container
    # ------------------------------------------------------------------
    load_medallion_task = BashOperator(
        task_id="load_bronze_silver",
        env={**os.environ},
        bash_command=f"""
            RAW_PATH="{{{{ ti.xcom_pull(task_ids='consume_from_kafka') }}}}"
            FILENAME=$(basename "$RAW_PATH")
            echo "Loading file: $FILENAME"
            docker exec {SPARK_CONTAINER} \\
                {SPARK_EXEC} {WORK_DIR}/src/jobs/tiki_load_iceberg.py \\
                --raw_file {WORK_DIR}/data/$FILENAME
        """,
        doc_md="""
### load_bronze_silver
Runs Spark job `tiki_load_iceberg.py` inside the `tiki_spark_crawler` container.
- **Bronze**: appends all raw rows to `tiki_bronze.products_raw`
- **Silver History**: detects price changes (SCD Type 4) → appends to `tiki.price_history`
- **Silver Active**: MERGE INTO `tiki.products` (SCD Type 1 — always holds the latest state)
""",
    )

    # ------------------------------------------------------------------
    # Task 4: Transform Gold — compute aggregates → Iceberg + Reporting Postgres
    # ------------------------------------------------------------------
    transform_gold_task = BashOperator(
        task_id="transform_gold",
        env={**os.environ},
        bash_command=f"""
            echo "Running Gold transformation ..."
            docker exec {SPARK_CONTAINER} \\
                {SPARK_EXEC} {WORK_DIR}/src/jobs/tiki_gold.py
        """,
        doc_md="""
### transform_gold
Runs Spark job `tiki_gold.py` to compute 5 Gold tables:
- `brand_performance` — Top brands by sales volume and rating
- `price_trend` — Average price by day and category
- `discount_analysis` — Discount rate analysis by category
- `top_products` — Top 100 best products
- `daily_summary` — Daily KPI overview for the dashboard

Each table is written to both Iceberg (Gold layer) and Reporting Postgres (Superset).
""",
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    extract_task >> consume_task >> load_medallion_task >> transform_gold_task
