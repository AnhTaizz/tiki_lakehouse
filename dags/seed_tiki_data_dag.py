"""
seed_tiki_data_dag.py
========================
Standalone DAG to trigger the full Tiki crawl.
This DAG does not run on a schedule because a full crawl takes 8-12 hours
and carries a high risk of IP banning by Tiki's Anti-Bot system.
Trigger this DAG manually only when necessary to refresh the backend data.
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
    "email_on_failure": False,
    "email_on_success": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
}

AIRFLOW_SRC = "/opt/airflow/src"

CATEGORIES = [
    {"id": 8322, "name": "Nhà Sách Tiki"}, # ~140k products
    {"id": 1883, "name": "Nhà Cửa - Đời Sống"}, # ~74k products
    {"id": 1815, "name": "Thiết Bị Số - Phụ Kiện Số"}, # ~20k products
    {"id": 1520, "name": "Làm Đẹp - Sức Khỏe"}, # ~17k products
    {"id": 27498, "name": "Phụ kiện thời trang"}, # ~12k products
    {"id": 4384, "name": "Bách Hóa Online"}, # ~11.2k products
    {"id": 1882, "name": "Điện Gia Dụng"}, # ~11.1k products
    {"id": 1846, "name": "Laptop - Máy Vi Tính - Linh kiện"}, # ~10.4k products
    {"id": 2549, "name": "Đồ Chơi - Mẹ & Bé"}, # ~10k products
    {"id": 8594, "name": "Ô Tô - Xe Máy - Xe Đạp"}, # ~8.8k products
    {"id": 931, "name": "Thời trang nữ"}, # ~7.5k products
    {"id": 8371, "name": "Đồng hồ và Trang sức"}, # ~6.8k products
    {"id": 915, "name": "Thời trang nam"}, # ~4.5k products
    {"id": 1975, "name": "Thể Thao - Dã Ngoại"}, # ~4.1k products
    {"id": 6000, "name": "Balo và Vali"}, # ~2.5k products
    {"id": 1686, "name": "Giày - Dép nam"}, # ~2.5k products
    {"id": 4221, "name": "Điện Tử - Điện Lạnh"}, # ~1.8k products
    {"id": 27616, "name": "Túi thời trang nam"}, # ~1.7k products
    {"id": 15078, "name": "Chăm sóc nhà cửa"}, # ~1.5k products
    {"id": 976, "name": "Túi thời trang nữ"}, # ~1.5k products
    {"id": 1801, "name": "Máy Ảnh - Máy Quay Phim"}, # ~1.3k products
    {"id": 1703, "name": "Giày - Dép nữ"}, # ~1.2k products
    {"id": 44792, "name": "NGON"}, # ~1.1k products
    {"id": 11312, "name": "Voucher - Dịch vụ"}, # ~378 products
    {"id": 1789, "name": "Điện Thoại - Máy Tính Bảng"}, # ~151 products
    {"id": 17166, "name": "Cross Border - Hàng Quốc Tế"} # ~27 products
]

with DAG(
    "seed_tiki_data_dag",
    default_args=default_args,
    description="Manual Trigger DAG to crawl ALL 26 Tiki categories (Takes 8-12 hours)",
    schedule_interval=None,  # Do not run automatically
    catchup=False,
    max_active_runs=1,
    tags=["tiki", "crawler", "manual"],
    doc_md="""
## Tiki Full Crawler

> **WARNING:** This DAG will initiate a full crawl of all 26 Tiki categories (~400,000+ items).
> This process will take roughly **8-15 hours**.
> There is a **HIGH RISK** that Tiki's Anti-Bot system will temporarily or permanently **BAN your IP**.

This DAG uses Dynamic Task Mapping to spawn 26 concurrent crawler tasks for each category.
""",
) as dag:

    commands = [
        f"python /opt/airflow/scripts/seed_tiki_data.py --force_full --category_ids {cat['id']}"
        for cat in CATEGORIES
    ]

    crawl_full_tiki_task = BashOperator.partial(
        task_id="run_full_crawler",
        env={
            **os.environ,
            "PYTHONPATH": AIRFLOW_SRC,
        },
    ).expand(bash_command=commands)

