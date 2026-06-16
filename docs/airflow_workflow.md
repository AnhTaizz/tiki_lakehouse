# Airflow Workflow — Từ Yêu Cầu Business đến Job Chạy Hàng Ngày

## Tổng quan

Tài liệu này mô tả **toàn bộ quy trình** từ khi nhận được một yêu cầu business
cho đến khi có một Airflow DAG chạy tự động hàng ngày/hàng giờ.

---

## 1. Lifecycle Đầy Đủ

```
┌─────────────────────────────────────────────────────────────────┐
│                   BUSINESS REQUIREMENT                          │
│  "Tôi muốn biết top brand đang bán chạy nhất mỗi ngày"          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA ANALYSIS                               │
│  - Cần data gì? → quantity_sold, brand_name, crawl_date         │
│  - Data đang ở đâu? → Silver table: tiki.products               │
│  - Output dạng gì? → Aggregate table, dashboard chart           │
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
│  - File được mount vào: /opt/airflow/dags/                      │
│  - Airflow Scheduler scan mỗi 30 giây                           │
│  - DAG hiện trong UI tự động — KHÔNG cần restart                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                  JOB CHẠY THEO SCHEDULE                          │
│  - Mỗi 4 tiếng (8h,12h,16h,20h,0h,4h ICT):                       │
│      crawl → Kafka → consume → Bronze/Silver → Gold              │
│  - Hourly (mỗi giờ): transform_gold (refresh Gold only)          │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│              BUSINESS USER NHÌN VÀO DASHBOARD                    │
│  Superset (http://localhost:8088)                                │
│  → Brand Performance chart                                       │
│  → Price Trend timeline                                          │
│  → Discount Analysis heatmap                                     │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Cấu Trúc Thư Mục & Vai Trò

```
tiki_lakehouse/
├── src/
│   ├── common/                  # Shared utilities (HTTP client, category/product API)
│   │   ├── http_client.py       # Retry-enabled HTTP client với browser impersonation
│   │   ├── tiki_category.py     # Crawl & parse category tree từ Tiki API
│   │   ├── tiki_product.py      # Crawl products theo category, có dedup
│   │   └── utils.py             # Logger, JSON saver
│   └── jobs/                    # Executable Spark/Python jobs — mỗi file = 1 nhiệm vụ
│       ├── tiki_extract.py      # [Producer] Crawl Beauty products → publish lên Kafka topic
│       ├── kafka_consumer.py    # [Consumer] Consume Kafka topic → save JSON file
│       ├── tiki_load_iceberg.py # [Load]     Bronze + Silver Iceberg tables (Spark)
│       └── tiki_gold.py         # [Transform] Gold aggregates → Iceberg + Postgres (Spark)
│
├── dags/
│   ├── tiki_pipeline_dag.py     # DAG mỗi 4h: Producer → Consumer → Bronze/Silver → Gold
│   └── tiki_gold_hourly_dag.py  # DAG mỗi giờ: refresh Gold tables (không crawl lại)
│
├── docs/
│   └── airflow_workflow.md      # Tài liệu này
│
├── notebooks/               # Exploratory notebooks (crawl, load, Iceberg study)
├── data/                    # Raw JSON extracts (git-ignored)
└── docker/                  # Các thư mục chứa cấu hình và Dockerfile
    ├── airflow/                 # Dockerfile cho Airflow custom image
    ├── hive/                    # Dockerfile cho Hive Metastore
    ├── superset/                # Dockerfile và config cho Superset
    └── shared/
        ├── spark-defaults.conf      # Spark packages, Iceberg catalog, MinIO settings
        └── core-site.xml            # S3A filesystem config (credentials inject qua docker-compose env)
```

---

## 3. Hướng Dẫn Thêm Job Mới (Quy Trình Chuẩn)

### Bước 1 — Xác định yêu cầu

```
Câu hỏi cần trả lời:
✓ Output là gì? (table, file, alert, ...)
✓ Input từ đâu? (Bronze, Silver, Gold, API, ...)
✓ Chạy bao thường? (daily, hourly, on-demand, ...)
✓ Phụ thuộc task nào chạy trước?
```

### Bước 2 — Viết Python job

```python
# src/jobs/ten_job_moi.py
from common.utils import setup_logger

logger = setup_logger(__name__)

def run():
    # ... logic của job
    pass

if __name__ == "__main__":
    run()
```

> **Convention**: Mỗi job là 1 file độc lập, chạy được trực tiếp bằng `python job.py`,
> không cần Airflow để test.

### Bước 3 — Test job thủ công trước

```bash
# Test trực tiếp trên máy local
PYTHONPATH=src python src/jobs/ten_job_moi.py

# Hoặc trong Spark container
docker exec tiki_spark_crawler python /home/jovyan/work/src/jobs/ten_job_moi.py
```

### Bước 4 — Thêm task vào DAG

```python
# dags/tiki_pipeline_dag.py (hoặc tạo DAG mới)
new_task = BashOperator(
    task_id="ten_task_moi",
    bash_command="docker exec tiki_spark_crawler python /home/jovyan/work/src/jobs/ten_job_moi.py",
    doc_md="### ten_task_moi\nMô tả ngắn gọn task này làm gì.",
)

# Kết nối vào chain
previous_task >> new_task >> next_task
```

### Bước 5 — Airflow tự phát hiện (không cần restart!)

```
Sau khi save file trong dags/:
→ Airflow Scheduler scan lại sau tối đa 30 giây
→ DAG / task mới xuất hiện trong UI tự động
→ Có thể trigger thủ công ngay để test
```

---

## 4. Monitoring & Troubleshooting

### Xem trạng thái pipeline

| Nơi xem | URL | Thông tin |
|---|---|---|
| Airflow UI | http://localhost:8081 | DAG runs, task status, logs |
| Spark UI | http://localhost:4040 | Spark jobs, stages, memory |
| MinIO Console | http://localhost:9001 | Iceberg files trong S3 |
| Superset Dashboard | http://localhost:8088 | Business dashboards |

### Khi task bị fail

```
1. Vào Airflow UI → DAG → Graph View
2. Click vào task màu đỏ → "Log"
3. Tìm dòng ERROR trong log
4. Fix code trong src/jobs/
5. Airflow retry tự động (retries=2, retry_delay=5m)
   hoặc Clear + Re-run thủ công
```

### Kiểm tra data đã load

```python
# Trong Jupyter Notebook (http://localhost:8888)
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

## 5. Kiến Trúc Medallion (Tổng Quan)

```
RAW JSON Files (local data/)
        │
        ▼
[Bronze Layer] local_catalog.tiki_bronze.products_raw
  - Append-only, không transform
  - Schema: đúng như raw API response
  - Partition by crawl_date
        │
        ▼
[Silver Layer]
  ├── local_catalog.tiki.products          (SCD Type 1 — Active state)
  │     MERGE INTO: luôn giữ state mới nhất
  │
  └── local_catalog.tiki.price_history     (SCD Type 4 — Full history)
        Chỉ append khi price/discount thay đổi
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
  → Business users xem được trực tiếp, không cần biết SQL
```

---

## 6. Schedule Reference

| DAG | Schedule | Mục đích |
|---|---|---|
| `tiki_beauty_lakehouse_pipeline` | `0 1,5,9,13,17,21 * * *` (mỗi 4h ICT) | Full pipeline: crawl → Kafka → Bronze → Silver → Gold |
| `tiki_gold_hourly_refresh` | `0 * * * *` (mỗi giờ) | Refresh Gold tables từ Silver đang có |

> **Lý do tách 2 DAG**: Crawl + Kafka ingestion tốn 30-45 phút và có risk bị rate-limit.
> Gold refresh chỉ tốn 2-5 phút. Tách ra giúp dashboard luôn fresh mà không phải crawl lại.

> **Lý do chọn 4h thay vì daily**: Giá sản phẩm e-commerce thay đổi nhiều lần trong ngày.
> Crawl mỗi 4h giúp price_history bắt được nhiều biến động hơn, làm dữ liệu phân tích giá chính xác hơn.
