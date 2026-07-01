"""
tiki_pipeline_dag.py
====================
Main DAG: runs every 4 hours (ICT)
  Task 1 — extract_and_publish : crawl Tiki API → publish each product to Kafka topic
  Task 2 — consume_from_kafka  : consume Kafka → collect into raw JSON file
  Task 3 — load_bronze_task     : Spark reads JSON → Bronze Iceberg
  Task 4 — clean_and_load_silver: Spark reads Bronze → Cleans Data → Silver Iceberg
  Task 5 — transform_gold       : Spark computes Gold aggregates → Iceberg + Reporting Postgres

Flow: extract_and_publish >> consume_from_kafka >> load_bronze_task >> clean_and_load_silver_task >> transform_gold
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
    "email": [os.environ.get("AIRFLOW__SMTP__SMTP_USER", "admin@localhost")],
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

SPARK_EXEC      = "/opt/conda/bin/python"
SPARK_CONTAINER = "tiki_spark_crawler"
import json

WORK_DIR        = "/home/jovyan/work"
AIRFLOW_SRC     = "/opt/airflow/src"

# Khôi phục cào 5 danh mục để test full-scale với Mock API:
CATEGORIES = [
    {"id": 1520, "name": "Làm Đẹp - Sức Khỏe"},
    {"id": 1846, "name": "Laptop - Máy Vi Tính"},
    {"id": 8322, "name": "Nhà Sách Tiki"},
    {"id": 1882, "name": "Điện Gia Dụng"},
    {"id": 931, "name": "Thời trang nữ"},
]

with DAG(
    "tiki_lakehouse_pipeline",
    default_args=default_args,
    description=(
        "Batch pipeline: crawl ALL Tiki categories every 4h → Kafka → Bronze → Silver → Gold → Superset"
    ),
    schedule_interval="0 */4 * * *",
    # every 4h: 08:00, 12:00, 16:00, 20:00, 00:00, 04:00 ICT (UTC+7)
    catchup=False,
    max_active_runs=1,
    tags=["tiki", "lakehouse", "kafka", "batch"],
    doc_md="""
## Tiki Lakehouse — Batch Pipeline

### Flow
```
[Every 4h ICT] Crawl Tiki API
    → Kafka Topic: tiki.raw.products  (batch ingestion)
    → Consumer: collect products → raw JSON file
    → Bronze Iceberg (raw append)
    → Data Cleaning (in-memory Spark)
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
    # Task 1: Extract & Publish (Mocked Crawler with Dynamic Task Mapping)
    # ------------------------------------------------------------------
    commands = [
        f"python {AIRFLOW_SRC}/jobs/tiki_extract.py --category_id {cat['id']} --category_name '{cat['name']}'"
        for cat in CATEGORIES
    ]

    extract_task = BashOperator.partial(
        task_id="extract_and_publish",
        pool="tiki_api_pool",
        env={
            **os.environ,
            "PYTHONPATH": AIRFLOW_SRC,
        },
        doc_md="""
### extract_and_publish
Sử dụng Dynamic Task Mapping (Airflow 2.3+) để chạy song song nhiều danh mục.
Bị giới hạn bởi pool `tiki_api_pool` để kiểm soát concurrency.
"""
    ).expand(bash_command=commands)

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
            CRAWL_DATE="{{{{ ds }}}}"
            echo "Consuming Kafka for crawl_date: $CRAWL_DATE"
            python {AIRFLOW_SRC}/jobs/kafka_consumer.py --crawl_date "$CRAWL_DATE"
        """,
        do_xcom_push=True,
        doc_md="""
### consume_from_kafka
Consumes all messages from Kafka topic `tiki.raw.products`.
Collects them into file `data/tiki_products_raw_YYYY-MM-DD.json`.
XCom push: absolute path of the raw file for Task 3 (Spark) to read.
""",
    )

    # ------------------------------------------------------------------
    # Task 3: Load Bronze — Spark job on Spark container
    # ------------------------------------------------------------------
    load_bronze_task = BashOperator(
        task_id="load_bronze_task",
        env={**os.environ},
        bash_command=f"""
            RAW_PATH="{{{{ ti.xcom_pull(task_ids='consume_from_kafka') }}}}"
            FILENAME=$(basename "$RAW_PATH")
            echo "Loading Bronze for file: $FILENAME"
            docker exec -e SPARK_OPTS="--driver-java-options=-Xmx1536M" {SPARK_CONTAINER} \\
                {SPARK_EXEC} {WORK_DIR}/src/jobs/tiki_load_iceberg.py \\
                --raw_file {WORK_DIR}/data/$FILENAME --layer bronze
        """,
        doc_md="""
### load_bronze_task
Runs Spark job `tiki_load_iceberg.py --layer bronze` inside the container.
- **Bronze**: overwrites today's partition in `tiki_bronze.products_raw` with raw JSON data.
""",
    )

    # ------------------------------------------------------------------
    # Task 3.5: Clean Data & Load Silver — Spark job on Spark container
    # ------------------------------------------------------------------
    clean_and_load_silver_task = BashOperator(
        task_id="clean_and_load_silver_task",
        env={**os.environ},
        bash_command=f"""
            RAW_PATH="{{{{ ti.xcom_pull(task_ids='consume_from_kafka') }}}}"
            FILENAME=$(basename "$RAW_PATH")
            echo "Cleaning & Loading Silver for file: $FILENAME"
            docker exec -e SPARK_OPTS="--driver-java-options=-Xmx1536M" {SPARK_CONTAINER} \\
                {SPARK_EXEC} {WORK_DIR}/src/jobs/tiki_load_iceberg.py \\
                --raw_file {WORK_DIR}/data/$FILENAME --layer silver
        """,
        doc_md="""
### clean_and_load_silver_task
Runs Spark job `tiki_load_iceberg.py --layer silver` inside the container.
- Reads raw data from Bronze layer
- **Data Cleaning**: drops null IDs, fills null prices with 0, trims strings
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
            docker exec -e SPARK_OPTS="--driver-java-options=-Xmx1536M" {SPARK_CONTAINER} \\
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
    # Task 0: Test Email (Always fails)
    # ------------------------------------------------------------------
    ENABLE_TEST_EMAIL = False

    if ENABLE_TEST_EMAIL:
        test_email_task = BashOperator(
            task_id="test_email_alert",
            bash_command="echo 'This task is designed to fail to test email alerts!' && exit 1",
            doc_md="This task intentionally fails to trigger an email alert.",
            retries=0,  # Disable retry to trigger email alert immediately
        )
        test_email_task >> extract_task >> consume_task >> load_bronze_task >> clean_and_load_silver_task >> transform_gold_task
    else:
        # Normal execution flow (Success path):
        extract_task >> consume_task >> load_bronze_task >> clean_and_load_silver_task >> transform_gold_task
