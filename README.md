# Tiki Beauty & Health Data Lakehouse

A local data lakehouse project that crawls **Tiki Beauty & Health** product data every 4 hours,
streams it through **Apache Kafka**, processes it via a **Medallion architecture** (Bronze → Silver → Gold),
and presents business insights via a **Superset dashboard**.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  Airflow (every 4h + hourly Gold refresh)                                    │
│  ┌─────────────┐  ┌───────────┐  ┌─────────────┐  ┌────────────────┐         │
│  │   Extract   │→ │   Kafka   │→ │Bronze+Silver│→ │  Gold Layer    │         │
│  │ Tiki API    │  │  Producer │  │  Iceberg    │  │Iceberg+Postgres│         │
│  └─────────────┘  └───────────┘  └─────────────┘  └────────┴───────┘         │
└──────────────────────────────────────────────────────────────────────────────┘
                                                              │
                                                              ▼
                                                     ┌──────────────────┐
                                                     │    Superset      │
                                                     │  localhost:8088  │
                                                     │  Business Charts │
                                                     └──────────────────┘
```

### Stack

| Component | Service | Role |
|---|---|---|
| **Ingestion** | Python + Apache Kafka | Crawl Tiki API → stream vào Kafka topic |
| **Compute** | Spark 3.5.0 (Jupyter/PySpark) | Data processing (Bronze/Silver/Gold) |
| **Storage** | MinIO (S3-compatible) | Iceberg warehouse |
| **Catalog** | Hive Metastore + PostgreSQL | Iceberg table metadata |
| **Orchestration** | Apache Airflow 2.8.1 | DAG scheduling (mỗi 4h + hourly Gold) |
| **Dashboard** | Apache Superset | Business BI |
| **Reporting DB** | PostgreSQL (reporting_db) | Gold tables for Superset |
| **Superset DB** | PostgreSQL (superset_db) | Superset internal metadata |

## Medallion Data Model

### Bronze — `local_catalog.tiki_bronze.products_raw`
Raw append-only table. Schema khớp với Tiki API response. Partition by `crawl_date`.

| Column         | Type   | Description                     |
|----------------|--------|---------------------------------|
| id             | LONG   | Product ID                      |
| sku            | STRING | SKU code                        |
| name           | STRING | Product name                    |
| price          | LONG   | Current price (VND)             |
| original_price | LONG   | Original price before discount  |
| discount_rate  | INT    | Discount percentage (%)         |
| brand_name     | STRING | Brand                           |
| rating_average | DOUBLE | Average star rating             |
| review_count   | INT    | Number of reviews               |
| quantity_sold  | INT    | Units sold                      |
| category_id    | INT    | Leaf category ID                |
| crawl_date     | DATE   | Date crawled                    |

### Silver — `local_catalog.tiki.products` (SCD Type 1)
Active product state — always the latest snapshot via `MERGE INTO`.

### Silver — `local_catalog.tiki.price_history` (SCD Type 4)
Append-only price change history — only rows where price/discount changed.

### Gold — `local_catalog.tiki_gold.*` (Business Aggregates)

| Table                  | Business Question                        |
|------------------------|------------------------------------------|
| `brand_performance`    | Brand nào đang bán chạy nhất?            |
| `price_trend`          | Giá thay đổi thế nào theo thời gian?     |
| `discount_analysis`    | Category nào đang giảm giá sâu nhất?     |
| `top_products`         | Top 100 sản phẩm đáng mua nhất?          |
| `daily_summary`        | Tổng quan KPI hàng ngày                  |

## Quick Start

### 1. Setup environment

```bash
cp .env.example .env
```

### 2. Start the stack

```bash
docker compose up -d --build
```

### 3. Access services

| Service         | URL                      | Login                   |
|-----------------|--------------------------|-------------------------|
| Airflow UI      | http://localhost:8081    | admin / password123     |
| Jupyter Notebook| http://localhost:8888    | *(no auth)*             |
| MinIO Console   | http://localhost:9001    | Xem `.env`              |
| Superset        | http://localhost:8088    | admin / admin123        |
| **Kafka UI**    | **http://localhost:8090**| *(no auth)*             |

### 4. Setup Superset — kết nối data source (lần đầu)

1. Vào **http://localhost:8088** → Login: `admin` / `admin123`
2. Menu trên cùng: **Settings → Database Connections → + Database**
3. Chọn **PostgreSQL**, điền thông tin:
   - **Host**: `reporting-postgres`
   - **Port**: `5432`
   - **Database**: `reporting`
   - **Username**: `reporting`
   - **Password**: `reporting123`
4. Click **Test Connection** → **Connect**
5. Vào **SQL Lab** để query thử:
   ```sql
   SELECT * FROM brand_performance ORDER BY total_quantity_sold DESC LIMIT 10;
   ```
6. Tạo Chart từ SQL Lab → Save vào Dashboard

## Pipeline DAGs

| DAG  | Schedule | Mô tả |
|---|---|---|
| `tiki_beauty_lakehouse_pipeline` | Mỗi 4 tiếng (8h,12h,16h,20h,0h,4h ICT) | Full pipeline: crawl → Kafka → Bronze → Silver → Gold |
| `tiki_gold_hourly_refresh` | Mỗi giờ | Chỉ refresh Gold + Superset |

### Quy trình thêm job mới

Xem **[docs/airflow_workflow.md](docs/airflow_workflow.md)** để biết quy trình đầy đủ từ:
> Yêu cầu business → Viết Python job → Định nghĩa DAG task → Auto-deploy → Monitor

## Project Layout

```
tiki_lakehouse/
├── src/
│   ├── common/           # Shared: HTTP client, category/product crawlers
│   └── jobs/
│       ├── tiki_extract.py      # Kafka Producer: Crawl Tiki API → publish to Kafka topic
│       ├── kafka_consumer.py    # Kafka Consumer: consume topic → save JSON file
│       ├── tiki_load_iceberg.py # Spark: Load Bronze + Silver Iceberg tables
│       └── tiki_gold.py         # Spark: Compute Gold aggregates → Iceberg + Postgres
├── dags/
│   ├── tiki_pipeline_dag.py     # Batch DAG (4 tasks, mỗi 4h)
│   └── tiki_gold_hourly_dag.py  # Hourly Gold refresh DAG
├── docs/
│   ├── airflow_workflow.md      # Airflow workflow documentation
│   ├── kafka_integration.md     # Kafka integration documentation
│   └── superset_guide.md        # Superset setup guide
├── docker/
│   ├── airflow/                 # Dockerfile cho Airflow custom image
│   ├── hive/                    # Dockerfile cho Hive Metastore
│   ├── superset/                # Dockerfile và config cho Superset
│   └── shared/                  # Spark + Iceberg + MinIO config (core-site.xml, v.v.)
├── notebooks/                   # Exploratory notebooks
├── data/                        # Raw JSON extracts (git-ignored)
└── docker-compose.yml
```

## Manual Commands

```bash
# Rebuild Airflow image sau khi thêm thư viện mới (ví dụ: kafka-python)
docker compose build --no-cache airflow-init airflow-webserver airflow-scheduler

# Chạy Producer thủ công (trong Airflow container)
KAFKA_BROKER=kafka:9092 PYTHONPATH=src python src/jobs/tiki_extract.py

# Chạy Consumer thủ công
KAFKA_BROKER=kafka:9092 PYTHONPATH=src python src/jobs/kafka_consumer.py --crawl_date 2026-06-16

# Kiểm tra Kafka topic có data chưa
docker exec tiki_kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic tiki.raw.products \
  --from-beginning --max-messages 3

# Chạy Bronze/Silver load trong Spark container
docker exec tiki_spark_crawler python /home/jovyan/work/src/jobs/tiki_load_iceberg.py \
  --raw_file /home/jovyan/work/data/<filename>.json

# Chạy Gold transform trong Spark container
docker exec tiki_spark_crawler python /home/jovyan/work/src/jobs/tiki_gold.py
```
