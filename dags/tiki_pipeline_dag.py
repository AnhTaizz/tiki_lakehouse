"""
tiki_pipeline_dag.py
====================
DAG chính: chạy hàng ngày lúc 15:00 (ICT)
  Task 1 — extract_tiki_data  : crawl Tiki API → raw JSON file
  Task 2 — load_bronze_silver : Bronze + Silver Iceberg tables
  Task 3 — transform_gold     : Gold aggregates → Iceberg + Reporting Postgres

Flow: extract_tiki_data >> load_bronze_silver >> transform_gold
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

SPARK_EXEC = "/opt/conda/bin/python"
SPARK_CONTAINER = "tiki_spark_crawler"
WORK_DIR = "/home/jovyan/work"

with DAG(
    "tiki_beauty_lakehouse_pipeline",
    default_args=default_args,
    description=(
        "Daily pipeline: crawl Tiki Beauty → Bronze → Silver → Gold → Superset"
    ),
    schedule_interval="0 15 * * *",  # 15:00 ICT (UTC+7)
    catchup=False,
    tags=["tiki", "lakehouse", "beauty", "daily"],
    doc_md="""
## Tiki Beauty Lakehouse — Daily Pipeline

### Flow
```
[15:00 ICT] Crawl Tiki API
    → Bronze Iceberg (raw append)
    → Silver Iceberg (SCD Type 1 active + SCD Type 4 price history)
    → Gold Iceberg + Reporting Postgres (Superset dashboards)
```

### Thêm DAG mới
1. Viết job Python trong `src/jobs/`
2. Định nghĩa task BashOperator gọi job đó
3. Kết nối task vào dependency chain với `>>`
4. Commit và Airflow tự phát hiện DAG mới

### Troubleshooting
- Xem log từng task trong Airflow UI → Graph View → Click task → Log
- Bronze table: `local_catalog.tiki_bronze.products_raw`
- Silver tables: `local_catalog.tiki.products`, `local_catalog.tiki.price_history`
- Gold tables: `local_catalog.tiki_gold.*`
""",
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Extract — Crawl Tiki API → raw JSON
    # ------------------------------------------------------------------
    extract_task = BashOperator(
        task_id="extract_tiki_data",
        env={
            **os.environ,
            "PYTHONPATH": "/opt/airflow/src",
        },
        bash_command="python /opt/airflow/src/jobs/tiki_extract.py",
        do_xcom_push=True,
        doc_md="""
### extract_tiki_data
Crawl toàn bộ category Beauty & Health từ Tiki API.
Output: raw JSON file tại `data/tiki_beauty_health_raw_YYYY-MM-DD.json`
XCom push: đường dẫn tuyệt đối của file raw để task sau dùng.
""",
    )

    # ------------------------------------------------------------------
    # Task 2: Load Bronze + Silver — Spark job trên Spark container
    # ------------------------------------------------------------------
    load_medallion_task = BashOperator(
        task_id="load_bronze_silver",
        env={**os.environ},
        bash_command=f"""
            RAW_PATH="{{{{ ti.xcom_pull(task_ids='extract_tiki_data') }}}}"
            FILENAME=$(basename "$RAW_PATH")
            echo "Loading file: $FILENAME"
            docker exec {SPARK_CONTAINER} \\
                {SPARK_EXEC} {WORK_DIR}/src/jobs/tiki_load_iceberg.py \\
                --raw_file {WORK_DIR}/data/$FILENAME
        """,
        doc_md="""
### load_bronze_silver
Chạy Spark job `tiki_load_iceberg.py` bên trong container `tiki_spark_crawler`.
- **Bronze**: append toàn bộ raw rows vào `tiki_bronze.products_raw`
- **Silver History**: phát hiện price change (SCD Type 4) → append vào `tiki.price_history`
- **Silver Active**: MERGE INTO `tiki.products` (SCD Type 1 — luôn có state mới nhất)
""",
    )

    # ------------------------------------------------------------------
    # Task 3: Transform Gold — tính aggregate → Iceberg + Reporting Postgres
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
Chạy Spark job `tiki_gold.py` để tính 5 Gold tables:
- `brand_performance` — Top brands theo doanh số và rating
- `price_trend` — Giá trung bình theo ngày và category
- `discount_analysis` — Phân tích discount rate
- `top_products` — Top 100 sản phẩm tốt nhất
- `daily_summary` — Tổng quan hàng ngày cho dashboard

Mỗi table được ghi ra cả Iceberg (Gold layer) lẫn Reporting Postgres (Superset).
""",
    )

    # ------------------------------------------------------------------
    # Dependency chain
    # ------------------------------------------------------------------
    extract_task >> load_medallion_task >> transform_gold_task
