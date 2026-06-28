# Tiki Lakehouse

A comprehensive local data lakehouse project that crawls **Tiki 5 Categories** product data every 4 hours, streams it through **Apache Kafka**, processes it via a **Medallion Architecture** (Bronze → Silver → Gold) using Apache Spark, and presents actionable business insights via an **Apache Superset** dashboard.

---

##  Table of Contents

- [1. Overview](#1-overview)
- [2. System Architecture & Tech Stack](#2-system-architecture--tech-stack)
- [3. Medallion Data Model](#3-medallion-data-model)
- [4. Prerequisites](#4-prerequisites)
- [5. Installation & Setup](#5-installation--setup)
- [6. Accessing the Services](#6-accessing-the-services)
- [7. Superset Dashboard Setup](#7-superset-dashboard-setup)
- [8. Pipeline DAGs Details](#8-pipeline-dags-details)
- [9. Project Layout](#9-project-layout)
- [10. Manual Commands & Troubleshooting](#10-manual-commands--troubleshooting)

---

## 1. Overview

This project implements an end-to-end Big Data pipeline for E-commerce analytics. It crawls product data from 5 main categories on Tiki: *Làm Đẹp - Sức Khỏe, Laptop - Máy Vi Tính, Nhà Sách Tiki, Điện Gia Dụng, Thời trang nữ*.

The pipeline is orchestrated by Airflow to run every 4 hours, ensuring the data lakehouse stays up-to-date with the latest product prices and discount rates.

---

## 2. System Architecture & Tech Stack

### Architecture Diagram

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│  Airflow Orchestration (every 4h)                                            │
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

### Tech Stack

| Component         | Technology                  | Role                                     |
|-------------------|-----------------------------|------------------------------------------|
| **Orchestration** | Apache Airflow 2.8.1        | DAG scheduling & task execution          |
| **Ingestion**     | Python + Apache Kafka       | Crawl Tiki API → stream to Kafka topic   |
| **Compute**       | Spark 3.5.0 (PySpark)       | Data processing (Bronze/Silver/Gold)     |
| **Storage**       | MinIO (S3-compatible)       | Underlying storage for Iceberg warehouse |
| **Catalog**       | Hive Metastore + PostgreSQL | Iceberg table metadata management        |
| **Reporting DB**  | PostgreSQL (reporting_db)   | Structured Gold tables for Superset BI   |
| **Dashboard**     | Apache Superset             | Business Intelligence & Visualizations   |

---

## 3. Medallion Data Model

### 🥉 Bronze — `local_catalog.tiki_bronze.products_raw`
Raw append-only table. Schema matches the Tiki API response exactly. Partitioned by `crawl_date`.

### 🥈 Silver — `local_catalog.tiki.products` (SCD Type 1)
Active product state — always holds the latest snapshot via `MERGE INTO`.

### 🥈 Silver — `local_catalog.tiki.price_history` (SCD Type 4)
Append-only price change history — records only when the price or discount rate changes.

### 🥇 Gold — `local_catalog.tiki_gold.*` (Business Aggregates)

| Table               | Business Question Answered                        |
|---------------------|---------------------------------------------------|
| `brand_performance` | Which brands are selling the most?                |
| `price_trend`       | How do prices change over time across categories? |
| `discount_analysis` | Which category has the deepest discounts?         |
| `top_products`      | What are the top 100 best products to buy?        |
| `daily_summary`     | Daily high-level KPI overview                     |

---

## 4. Prerequisites

To run this project, you need to have the following installed on your machine:
- **Docker** & **Docker Compose** (Ensure Docker engine is running and has at least 8GB of RAM allocated).
- **Git** (to clone the repository).

---

## 5. Installation & Setup

**Step 1: Clone the repository**
```bash
git clone <your-repo-url>
cd tiki_lakehouse
```

**Step 2: Setup Environment Variables**
Copy the example environment file to `.env`:
```bash
cp .env.example .env
```

**Step 3: Start the Docker Stack**
Build and start all services in detached mode:
```bash
docker compose up -d --build
```
> **Note:** The first time you run this, it will take several minutes to download all Docker images and initialize the Airflow metadata database.

---

## 6. Accessing the Services

Once all containers are up and running, you can access the various UIs using the credentials below:

| Service           | Access URL                                     | Default Login           |
|-------------------|------------------------------------------------|-------------------------|
| **Airflow UI**    | [http://localhost:8081](http://localhost:8081) | `admin` / `password123` |
| **Kafka UI**      | [http://localhost:8090](http://localhost:8090) | *(no auth)*             |
| **Jupyter Lab**   | [http://localhost:8888](http://localhost:8888) | *(no auth)*             |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | See `.env` file         |
| **Superset BI**   | [http://localhost:8088](http://localhost:8088) | `admin` / `password123` |

---

## 7. Superset Dashboard Setup

To visualize the Gold data in Superset for the first time:

1. Open **[http://localhost:8088](http://localhost:8088)** and login (`admin` / `password123`).
2. Navigate to **Settings (Top Menu) → Database Connections → + Database**.
3. Choose **PostgreSQL** and fill in the connection details:
   - **Host**: `reporting-postgres`
   - **Port**: `5432`
   - **Database Name**: `reporting`
   - **Username**: `reporting`
   - **Password**: `reporting123`
4. Click **Test Connection** → **Connect**.
5. Go to **SQL Lab** to verify the data:
   ```sql
   SELECT * FROM brand_performance ORDER BY total_quantity_sold DESC LIMIT 10;
   ```
6. You can now create Datasets from these tables and build your charts!

---

## 8. Pipeline DAGs Details

The entire ETL process is managed by Airflow in a single batch DAG.

| DAG Name                  | Cron Schedule            | Description                                                                   |
|---------------------------|--------------------------|-------------------------------------------------------------------------------|
| `tiki_lakehouse_pipeline` | `0 1,5,9,13,17,21 * * *` | Crawl 5 categories ➔ Kafka Producer ➔ Kafka Consumer ➔ Bronze ➔ Silver ➔ Gold |

> **Why every 4 hours?** E-commerce prices change frequently. Crawling every 4 hours helps the `price_history` table capture more intra-day fluctuations, resulting in a more accurate price trend analysis.

---

## 9. Project Layout

```text
tiki_lakehouse/
├── src/
│   ├── common/           # Shared utilities (HTTP client, category extractors)
│   └── jobs/             # Executable Spark & Python jobs
│       ├── tiki_extract.py      # [Producer] Crawls Tiki API & publishes to Kafka
│       ├── kafka_consumer.py    # [Consumer] Consumes Kafka topic & saves JSON file
│       ├── tiki_load_iceberg.py # [Spark] Loads Bronze & Silver Iceberg tables
│       └── tiki_gold.py         # [Spark] Computes Gold aggregates → Iceberg + Postgres
├── dags/
│   └── tiki_pipeline_dag.py     # Main Airflow DAG (4 Tasks, every 4h)
├── docs/                 # Detailed technical documentation
│   ├── airflow_workflow.md
│   ├── kafka_integration.md
│   └── superset_guide.md
├── docker/               # Dockerfiles & custom configurations
│   ├── airflow/
│   ├── hive/
│   ├── superset/
│   └── shared/
├── notebooks/            # Exploratory PySpark Jupyter notebooks
├── data/                 # Raw JSON extracts (git-ignored)
└── docker-compose.yml    # Main Docker compose stack
```

---

## 10. Manual Commands & Troubleshooting

### Restarting / Rebuilding
If you add new Python libraries (e.g., `kafka-python`), rebuild the Airflow image:
```bash
docker compose build --no-cache airflow-init airflow-webserver airflow-scheduler
```

### Running Jobs Manually
You can test individual scripts manually inside the respective containers:

**1. Run Kafka Producer (inside Airflow container):**
```bash
docker exec -it <airflow-scheduler-container-name> bash
KAFKA_BROKER=kafka:9092 PYTHONPATH=src python src/jobs/tiki_extract.py
```

**2. Run Kafka Consumer (inside Airflow container):**
```bash
KAFKA_BROKER=kafka:9092 PYTHONPATH=src python src/jobs/kafka_consumer.py --crawl_date 2026-06-16
```

**3. Check Kafka Topic Data:**
```bash
docker exec tiki_kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic tiki.raw.products \
  --from-beginning --max-messages 3
```

**4. Run Spark Bronze/Silver Load:**
```bash
docker exec tiki_spark_crawler python /home/jovyan/work/src/jobs/tiki_load_iceberg.py \
  --raw_file /home/jovyan/work/data/<filename>.json
```

**5. Run Spark Gold Transform:**
```bash
docker exec tiki_spark_crawler python /home/jovyan/work/src/jobs/tiki_gold.py
```
