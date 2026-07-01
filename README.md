# Tiki Lakehouse

An end-to-end Data Lakehouse pipeline orchestrated by **Apache Airflow**, simulating **Tiki E-commerce** product data via a Mock API, routing it through **Apache Kafka**, processing it via a **Medallion Architecture** (Bronze → Silver → Gold) using **Apache Spark**, and presenting actionable business insights via an **Apache Superset** dashboard.

---

## Table of Contents

- [1. Overview](#1-overview)
- [2. System Architecture & Tech Stack](#2-system-architecture--tech-stack)
- [3. Medallion Data Model](#3-medallion-data-model)
- [4. Prerequisites](#4-prerequisites)
- [5. Installation & Setup](#5-installation--setup)
- [6. Running the Pipeline (Step-by-Step)](#6-running-the-pipeline-step-by-step)
- [7. Accessing the Services](#7-accessing-the-services)
- [8. Superset Dashboard Setup](#8-superset-dashboard-setup)
- [9. Pipeline DAG Details](#9-pipeline-dag-details)
- [10. Project Layout](#10-project-layout)
- [11. Manual Commands & Troubleshooting](#11-manual-commands--troubleshooting)
- [12. Disaster Recovery & Auto-Recovery](#12-disaster-recovery--auto-recovery)

---

## 1. Overview

This project implements an end-to-end Big Data pipeline using a **Lambda Architecture**:

- **Batch Layer (every 4h):** Orchestrated by Airflow. Extracts data from a **Mock API** (simulating 5 Tiki categories: *Làm Đẹp - Sức Khỏe, Laptop - Máy Vi Tính, Nhà Sách Tiki, Điện Gia Dụng, Thời trang nữ*), routes it through Kafka, and processes it via Spark into Bronze → Silver → Gold Iceberg tables.
- **Speed Layer (real-time):** A Simulator generates live e-commerce events (Purchases, Flash Sales, Restocks) using a Sine-wave traffic pattern, which Spark Structured Streaming consumes and writes directly to a Reporting PostgreSQL database for live dashboarding.

---

## 2. System Architecture & Tech Stack

### Architecture Diagram

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│  Airflow Orchestration (Batch Layer — every 4h)                              │
│  ┌─────────────┐  ┌───────────┐  ┌─────────────┐  ┌────────────────┐        │
│  │   Extract   │→ │   Kafka   │→ │Bronze+Silver│→ │  Gold Layer    │        │
│  │  Mock Tiki  │  │  Producer │  │  Iceberg    │  │Iceberg+Postgres│        │
│  │    API      │  └───────────┘  └─────────────┘  └────────┬───────┘        │
│  └─────────────┘                                           │                │
└────────────────────────────────────────────────────────────│────────────────┘
          │                                                  │
          │                                                  │
   (Speed Layer)                                             ▼
          ▼                                          ┌──────────────────┐
  ┌───────────────┐        ┌───────────────────┐     │                  │
  │ Tiki Simulator│───────>│ Spark Structured  │────>│ Apache Superset  │
  │ (Live Events) │        │    Streaming      │     │ (BI Dashboard)   │
  └───────────────┘        └───────────────────┘     └──────────────────┘
```

### Tech Stack

| Component         | Technology                  | Role                                                       |
|-------------------|-----------------------------|------------------------------------------------------------|
| **Orchestration** | Apache Airflow 2.8.1        | DAG scheduling, task execution, email alerting             |
| **Ingestion**     | Python + Apache Kafka       | Extract from Mock Tiki API → publish to Kafka topic        |
| **Compute**       | Spark 3.5.0 (PySpark)       | ETL processing (Bronze/Silver/Gold) + Structured Streaming |
| **Storage**       | MinIO (S3-compatible)       | Underlying object storage for Iceberg warehouse            |
| **Catalog**       | Hive Metastore + PostgreSQL | Iceberg table metadata management                          |
| **Reporting DB**  | PostgreSQL (reporting_db)   | Gold tables + real-time events for Superset BI             |
| **Dashboard**     | Apache Superset             | Business Intelligence & real-time visualizations           |
| **Simulation**    | SQLite + Mock API (FastAPI) | Simulates 180,000+ Tiki products locally                   |

---

## 3. Medallion Data Model

### 🥉 Bronze — `local_catalog.tiki_bronze.products_raw`
Raw append-only table. Schema matches the Mock API response exactly. Partitioned by `crawl_date`.

### 🥈 Silver — `local_catalog.tiki_silver.products` (SCD Type 1)
Active product state — always holds the latest snapshot via `MERGE INTO`. Updated on every 4h batch run.

### 🥈 Silver — `local_catalog.tiki_silver.price_history` (SCD Type 4)
Append-only price change history — records a new row only when the price or discount rate changes.

### 🥇 Gold — `local_catalog.tiki_gold.*` (Business Aggregates)

| Table               | Business Question Answered                                    |
|---------------------|---------------------------------------------------------------|
| `brand_performance` | Which brands are selling the most?                            |
| `price_trend`       | How do prices change over time across categories?             |
| `discount_analysis` | Which category has the deepest discounts?                     |
| `top_products`      | What are the top 100 best products to buy now?                |
| `daily_summary`     | Daily high-level KPI overview (sales, events, product count)  |

### ⚡ Speed — `reporting_db.realtime_events` (PostgreSQL)
Written directly by Spark Structured Streaming (bypassing Iceberg) for sub-second latency dashboards in Superset.

---

## 4. Prerequisites

- **Docker** & **Docker Compose** (Ensure Docker engine has at least **8GB RAM** allocated).
- **Git**
- **Python 3.9+** installed on the host machine (for the Control Panel scripts).
- **Windows OS** (for the `tiki_control_panel.bat` script). On Linux/Mac, run the commands manually — see [Section 6](#6-running-the-pipeline-step-by-step).

> **RAM Note:** The full stack (Spark + Airflow + Kafka + MinIO + PostgreSQL × 3 + Superset) typically consumes ~6–7 GB of RAM under load. Allocating **8 GB** to Docker is the minimum; **12 GB** is recommended.

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
# Linux / Mac
cp .env.example .env

# Windows PowerShell
Copy-Item .env.example .env
```

Open `.env` and **replace all placeholder values** before continuing:

```env
# Replace these placeholder values with real passwords of your choice
HIVE_DB_PASSWORD=your_hive_db_password_here   # → e.g. hive_secret_123
AIRFLOW_DB_PASSWORD=your_airflow_db_password_here  # → e.g. airflow_secret_123
AIRFLOW_SECRET_KEY=your_airflow_secret_key_here    # → any random string

# MinIO credentials (default values are already set and ready to use)
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=minio123
```

*(Optional) To enable Airflow Email Alerts, also set your SMTP credentials:*
```env
AIRFLOW_SMTP_USER=your-email@gmail.com
AIRFLOW_SMTP_PASSWORD=your-app-password    # Gmail App Password (not your login password)
```

**Step 3: Install Host Python Dependencies**

The Mock API and Simulator scripts run **directly on your host machine** (not inside Docker). Install their dependencies:
```bash
pip install -r requirement.txt
```

**Step 4: Create necessary directories (Important for Linux/Mac)**

Docker might create these directories as `root` if they don't exist, which causes Permission Denied errors for Airflow. Create them manually first:
```bash
mkdir -p logs notebooks data/mock_data
```

**Step 5: Start the Docker Stack**

Build and start all services in detached mode:
```bash
docker compose up -d --build
```

> **Note:** The first run takes **5–15 minutes** to download all Docker images (~5 GB) and initialize the Airflow metadata database. Wait until all containers show `healthy` or `running` status before proceeding.

Monitor startup progress:
```bash
docker compose ps
```

---

## 6. Running the Pipeline (Step-by-Step)

The project includes `tiki_control_panel.bat` to simplify operation on Windows. Open a terminal **in the project root directory** and run:
```bash
tiki_control_panel.bat
```

> **Linux/Mac alternative:** Run each numbered step's command directly in your terminal.

---

### Step 1 — Initialize the Local Database *(Do this once)*

Select **[1] Init SQLite Database**. This loads 180,000+ simulated products from the `data/mock_data/` JSON shards into the local `data/tiki_backend.db` SQLite database used by the Mock API and Simulator.

```bash
# Linux/Mac equivalent:
python src/simulators/init_sqlite.py
```

> **Prerequisite:** The `data/mock_data.zip` file is included in this repository. When you run `init_sqlite.py`, it will automatically extract the JSON shards into `data/mock_data/` and insert 180,000+ products into the local SQLite database.

---

### Step 2 — Start the Mock API *(Required for Airflow Batch)*

Select **[2] Start Mock API Service**. This starts a local FastAPI server at `http://0.0.0.0:8000` that Airflow's extractor will call instead of the real Tiki server. **Keep this window open** for as long as you want to run batch jobs.

```bash
# Linux/Mac equivalent:
python src/simulators/mock_tiki_service.py
```

Verify it is running: open `http://localhost:8000/docs` in your browser to see the Swagger UI.

---

### Step 3 — Trigger the Airflow Batch Pipeline

1. Go to [http://localhost:8081](http://localhost:8081) and log in (`admin` / `password123`).
2. Find the `tiki_lakehouse_pipeline` DAG and **Unpause** it (toggle the switch on the left).
3. Click **Trigger DAG** (▶) to run it immediately.
4. Watch it flow through all 5 tasks: `extract_and_publish` → `consume_from_kafka` → `load_bronze_task` → `clean_and_load_silver_task` → `transform_gold`.

> **Important:** The Mock API (Step 2) must be running before triggering this DAG, otherwise `extract_and_publish` will fail.

---

### Step 4 — Start Real-Time Streaming

Select **[3] Start Real-time Streaming**. This opens 2 new windows:
- **TIKI SIMULATOR:** Generates live e-commerce events (PURCHASE, FLASH_SALE, UNPUBLISHED, RESTOCK) using a Sine-wave traffic model to simulate realistic peak/off-peak traffic. Connects to Kafka on `localhost:9093`.
- **SPARK STREAMING PROCESSOR:** Consumes events from Kafka every 20 seconds and writes them to `reporting_db.realtime_events` for live Superset charts.

```bash
# Linux/Mac equivalent (run each in a separate terminal):
# Terminal 1 — Simulator
KAFKA_BROKER=localhost:9093 python src/simulators/tiki_continuous_simulator.py

# Terminal 2 — Spark Streaming Processor
docker exec -it tiki_spark_crawler python /home/jovyan/work/src/jobs/tiki_stream_processor.py
```

---

## 7. Accessing the Services

Once all containers are up and running:

| Service           | Access URL                                     | Default Login                    |
|-------------------|------------------------------------------------|----------------------------------|
| **Airflow UI**    | [http://localhost:8081](http://localhost:8081) | `admin` / `password123`          |
| **Kafka UI**      | [http://localhost:8090](http://localhost:8090) | *(no auth)*                      |
| **Jupyter Lab**   | [http://localhost:8888](http://localhost:8888) | *(no auth, no token)*            |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `admin` / `minio123`             |
| **Superset BI**   | [http://localhost:8088](http://localhost:8088) | `admin` / `password123`          |
| **Mock API Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | *(started via BAT file)*|
| **Spark UI**      | [http://localhost:4040](http://localhost:4040) | *(available when a job is running)*|

---

## 8. Superset Dashboard Setup

To visualize data in Superset for the first time:

1. Open [http://localhost:8088](http://localhost:8088) and log in (`admin` / `password123`).
2. Navigate to **Settings → Database Connections → + Database**.
3. Choose **PostgreSQL** and fill in the connection details:
   - **Host**: `reporting-postgres`
   - **Port**: `5432`
   - **Database Name**: `reporting`
   - **Username**: `reporting`
   - **Password**: `reporting123`
4. Click **Test Connection** → **Connect**.
5. Go to **SQL Lab** to verify the data:
   ```sql
   -- Verify Gold (Batch) data
   SELECT * FROM brand_performance ORDER BY total_quantity_sold DESC LIMIT 10;

   -- Verify Speed (Real-time) data
   SELECT _event_type, COUNT(*) FROM realtime_events GROUP BY _event_type;
   ```
6. Create Datasets from these tables and build your charts!

---

## 9. Pipeline DAG Details

The entire Batch ETL is managed by Airflow in a single DAG with **5 sequential tasks**.

| DAG Name                  | Schedule              | Max Active Runs |
|---------------------------|-----------------------|-----------------|
| `tiki_lakehouse_pipeline` | `0 */4 * * *` (Every 4h) | 1           |

### Task Flow

```
extract_and_publish >> consume_from_kafka >> load_bronze_task >> clean_and_load_silver_task >> transform_gold
```

| Task ID                     | Runs In              | Description                                                                |
|-----------------------------|----------------------|----------------------------------------------------------------------------|
| `extract_and_publish`       | Airflow container    | Calls Mock API for each category (Dynamic Task Mapping, 15 workers). Publishes products to Kafka topic `tiki.raw.products`. |
| `consume_from_kafka`        | Airflow container    | Reads all Kafka messages → saves to `data/tiki_products_raw_YYYY-MM-DD.json`. |
| `load_bronze_task`          | Spark container      | Spark reads JSON → appends to Bronze Iceberg table (partitioned by `crawl_date`). |
| `clean_and_load_silver_task`| Spark container      | Cleans data → detects price changes (SCD4 `price_history`) → MERGE INTO active products (SCD1 `products`). |
| `transform_gold`            | Spark container      | Computes 5 Gold aggregations → writes to Iceberg Gold + Reporting Postgres for Superset. |

> **Retry Policy:** Each task retries up to 3 times with a 5-minute delay. Email alerts are sent on both success and failure via SMTP (if configured).

---

## 10. Project Layout

```text
tiki_lakehouse/
├── dags/
│   └── tiki_pipeline_dag.py        # Main Airflow DAG (5 Tasks, every 4h)
├── src/
│   ├── common/                     # Shared utilities (config, helpers)
│   ├── jobs/                       # Core data processing scripts
│   │   ├── tiki_extract.py         # [Task 1] Calls Mock API → publishes to Kafka
│   │   ├── kafka_consumer.py       # [Task 2] Consumes Kafka → saves JSON file
│   │   ├── tiki_load_iceberg.py    # [Task 3&4] Spark: JSON → Bronze & Silver Iceberg
│   │   ├── tiki_gold.py            # [Task 5] Spark: Gold aggregates → Iceberg + Postgres
│   │   └── tiki_stream_processor.py# [Speed Layer] Spark Structured Streaming → Postgres
│   ├── simulators/
│   │   ├── init_sqlite.py          # One-time: loads 180k+ products into SQLite
│   │   ├── mock_tiki_service.py    # FastAPI Mock API server (replaces real Tiki API)
│   │   └── tiki_continuous_simulator.py # Generates live Kafka events (Sine-wave traffic)
│   └── scripts/                    # Utility & maintenance scripts
├── docker/                         # Dockerfiles & service configurations
│   ├── airflow/
│   ├── hive/
│   ├── spark/
│   └── superset/
├── data/
│   ├── mock_data/                  # Pre-generated JSON shards (source for SQLite init)
│   └── *.json                      # Raw JSON extracts from pipeline runs (git-ignored)
├── notebooks/                      # Exploratory PySpark Jupyter notebooks
├── docs/                           # Additional project documentation
├── tiki_control_panel.bat          # One-click operation panel (Windows only)
├── docker-compose.yml              # Full Docker stack definition
├── requirement.txt                 # Host Python dependencies
└── .env.example                    # Environment variable template (copy to .env)
```

---

## 11. Manual Commands & Troubleshooting

### Checking Container Health
```bash
# See status of all containers
docker compose ps

# Follow logs of a specific service
docker compose logs -f airflow-scheduler
docker compose logs -f tiki_spark_crawler
```

### Restarting Services
```bash
# Restart Airflow Scheduler (if DAGs not picking up changes)
docker compose restart airflow-scheduler

# Rebuild Airflow image after adding Python dependencies
docker compose build --no-cache airflow-init airflow-webserver airflow-scheduler
```

### Running Spark Jobs Manually
```bash
# Run Bronze & Silver load (inside Spark container)
docker exec tiki_spark_crawler \
    python /home/jovyan/work/src/jobs/tiki_load_iceberg.py \
    --raw_file /home/jovyan/work/data/<filename>.json --layer bronze

# Run Gold transformation (inside Spark container)
docker exec tiki_spark_crawler \
    python /home/jovyan/work/src/jobs/tiki_gold.py

# Run Streaming Processor manually
docker exec -it tiki_spark_crawler \
    python /home/jovyan/work/src/jobs/tiki_stream_processor.py
```

### Inspecting Kafka Topic
```bash
# From INSIDE the Kafka container (internal port 9092)
docker exec tiki_kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic tiki.raw.products \
  --from-beginning --max-messages 5

# From your HOST machine (external port 9093)
# Requires Kafka CLI installed locally
kafka-console-consumer.sh \
  --bootstrap-server localhost:9093 \
  --topic tiki.raw.products \
  --from-beginning --max-messages 5
```

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `extract_and_publish` fails immediately | Mock API not running | Run `python src/simulators/mock_tiki_service.py` first |
| `load_bronze_task` hangs for >10 min | Hive Metastore not healthy | `docker compose restart hive-metastore` then re-trigger |
| Superset shows no data | Gold task hasn't run yet | Trigger the DAG and wait for all 5 tasks to succeed |
| Simulator can't connect to Kafka | Wrong broker address | Ensure `KAFKA_BROKER=localhost:9093` is set (host → external port) |
| `init_sqlite.py` finds 0 products | `data/mock_data/` is empty | Ensure `mock_*.json` shard files exist in `data/mock_data/` |

---

## 12. Disaster Recovery & Auto-Recovery

### Concurrent Write Protection (ACID Transactions)
Apache Iceberg uses **Merge-On-Read (MOR)** to handle simultaneous writes from the Batch and Streaming layers. If both write to the same table at the same time, Iceberg raises a `ValidationException` to prevent data corruption (no dirty reads, no partial writes).

### Streaming Auto-Recovery (Fault-Tolerance)
The Streaming Processor (`tiki_stream_processor.py`) is wrapped in a `while True / try-except` loop. If the process crashes for any reason, it will:
1. Log a warning message.
2. Wait 5 seconds.
3. Automatically restart from the **latest Kafka Checkpoint** — guaranteeing **Exactly-Once** semantics (no data loss, no duplicates).

### Airflow Task Retry
All DAG tasks are configured with `retries=3` and `retry_delay=5 minutes`. If a Spark job fails transiently (e.g., memory pressure), Airflow will automatically retry before sending a failure email alert.
