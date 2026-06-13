"""
tiki_gold_hourly_dag.py
=======================
DAG phụ: Refresh Gold tables mỗi giờ (không crawl lại, chỉ tính lại aggregates).

Mục đích:
- Superset dashboard luôn hiển thị số liệu gần real-time
- Tần suất cao hơn giúp demo được sự thay đổi theo thời gian

Flow:
  transform_gold (mỗi giờ, đọc Silver tables hiện có)

Lưu ý:
- DAG này KHÔNG crawl mới — chỉ re-compute Gold từ Silver đang có
- Crawl mới vẫn chỉ chạy 1 lần/ngày qua `tiki_beauty_lakehouse_pipeline`
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
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

SPARK_EXEC = "/opt/conda/bin/python"
SPARK_CONTAINER = "tiki_spark_crawler"
WORK_DIR = "/home/jovyan/work"

with DAG(
    "tiki_gold_hourly_refresh",
    default_args=default_args,
    description="Hourly Gold aggregation refresh — cập nhật Superset dashboard mỗi giờ",
    schedule_interval="0 * * * *",  # Mỗi giờ đầu giờ
    catchup=False,
    tags=["tiki", "gold", "hourly", "dashboard"],
    doc_md="""
## Tiki Gold Hourly Refresh

DAG này refresh các Gold tables mỗi giờ để Superset dashboard luôn cập nhật.

### Tại sao cần DAG riêng?
- Crawl data mất ~30-45 phút, không cần chạy mỗi giờ
- Tính lại aggregate từ Silver chỉ mất ~2-5 phút
- Dashboard luôn có data mới nhất mà không cần crawl lại

### Gold Tables được refresh
| Table | Mô tả |
|---|---|
| brand_performance | Top brands theo doanh số |
| price_trend | Xu hướng giá theo ngày |
| discount_analysis | Phân tích giảm giá |
| top_products | Top 100 sản phẩm |
| daily_summary | Tổng quan dashboard |
""",
) as dag:

    refresh_gold_task = BashOperator(
        task_id="refresh_gold_tables",
        env={**os.environ},
        bash_command=f"""
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting hourly Gold refresh ..."
            docker exec {SPARK_CONTAINER} \\
                {SPARK_EXEC} {WORK_DIR}/src/jobs/tiki_gold.py
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Gold refresh complete."
        """,
        doc_md="""
### refresh_gold_tables
Re-compute 5 Gold aggregate tables từ Silver Iceberg tables hiện có.
Ghi kết quả ra Reporting Postgres để Superset dashboard tự động cập nhật.
""",
    )
