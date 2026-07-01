# Kafka Integration — Architecture Guide

## 1. Overview

The Tiki Lakehouse project integrates **Apache Kafka** to establish a robust, decoupled **Lambda Architecture** (handling both Batch and Real-time streaming). This clearly separates ingestion from processing:

- **Ingestion** (Data Collection): Python + Kafka Producers (`tiki_extract.py` and `tiki_continuous_simulator.py`)
- **Processing** (Data Transformation): Apache Spark (`tiki_load_iceberg.py` for Batch, `tiki_stream_processor.py` for Streaming)

This pattern is a **Hybrid Architecture** — widely used in enterprise Data Engineering to ensure fault tolerance and high availability.

---

## 2. Pipeline Evolution

### Before Kafka (Monolithic Batch Pipeline)
```
[Airflow - Once a day]
  Task 1: tiki_extract.py  →  Saves JSON file to data/
  Task 2: tiki_load_iceberg.py  →  Spark reads JSON → Bronze → Silver
  Task 3: tiki_gold.py  →  Spark computes Gold → Superset
```

### After Kafka (Decoupled Pipeline)
```
[Airflow Batch - Every 4 hours]
  Task 1: tiki_extract.py (Producer)  →  Crawls Mock API → Publishes products to Kafka topic
  Task 2: kafka_consumer.py (Consumer)  →  Consumes Kafka → Batches into JSON file
  Task 3: tiki_load_iceberg.py  →  Spark reads JSON → Bronze
  Task 4: tiki_load_iceberg.py  →  Spark reads Bronze → Cleans Data → Silver
  Task 5: tiki_gold.py  →  Spark computes Gold → Superset

[Real-time Streaming - Continuous]
  Simulator (Producer)  →  Generates live events (Sales/Discounts) → Publishes to Kafka
  Spark Streaming (Consumer)  →  Consumes Kafka → Writes directly to Postgres (Speed Layer)
```

---

## 3. Core Kafka Concepts

### 3.1 Topic
A queue holding data. Think of it as a "channel" — producers send data in, consumers read data out.

```
Topic name: tiki.raw.products (Batch) / tiki.stream.products (Streaming)

Contents (each message is 1 product event):
  offset 0  → {"id": 123, "name": "Sunscreen", "price": 250000, "crawl_date": "2026-06-16", ...}
  offset 1  → {"id": 124, "name": "Lipstick", "price": 180000, ...}
  ...
```

### 3.2 Producer
The component that **sends** data to Kafka. In our project, this is `tiki_extract.py`.

```python
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",   # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # dict → bytes
    acks="all",    # Broker must acknowledge successful write
    retries=3,     # Auto-retry on failure
)
producer.send("tiki.raw.products", value=product_dict)
producer.flush()   # Ensure all messages are delivered
producer.close()
```

### 3.3 Consumer
The component that **reads** data from Kafka. In our batch pipeline, this is `kafka_consumer.py`.

```python
consumer = KafkaConsumer(
    "tiki.raw.products",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",    # Read from beginning (idempotent for re-runs)
    group_id="tiki-bronze-loader",   # Kafka tracks offsets by this group_id
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),  # bytes → dict
    consumer_timeout_ms=30_000,      # Auto-stop after 30s idle
)

for message in consumer:
    product = message.value   # Extract message content
```

### 3.4 Consumer Group
`group_id="tiki-bronze-loader"` — Kafka uses this ID to **remember the offset** (how far it has read). If a consumer crashes and restarts, it resumes from where it left off instead of starting over.

---

## 4. Key Configuration

### `docker-compose.yml`

```yaml
zookeeper:        # Manages Kafka cluster state
  image: confluentinc/cp-zookeeper:7.6.1
  environment:
    ZOOKEEPER_CLIENT_PORT: 2181

kafka:            # Kafka broker — stores topics and messages
  image: confluentinc/cp-kafka:7.6.1
  depends_on: [zookeeper]
  ports:
    - "9092:9092"   # Internal Docker port
    - "9093:9093"   # External Host port
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9093
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

*Note: We expose port `9093` externally so that scripts running on your host machine (like the Mock API Simulators) can connect to the Dockerized Kafka broker using `localhost:9093`.*

---

## 5. End-to-End Data Flow (Batch)

```
[08:00 ICT — Airflow triggers]
       │
       ▼ Task 1 (Airflow container)
tiki_extract.py
  - Calls Mock Tiki API (localhost:8000) for deterministic, ban-free data
  - For each product → producer.send("tiki.raw.products", product)
  - flush() → close()
  - print("2026-06-16")  → XCom
       │
       ▼ Task 2 (Airflow container)
kafka_consumer.py --crawl_date "2026-06-16"
  - Connects to Kafka broker at kafka:9092
  - Drains all messages from "tiki.raw.products" (stops after 30s idle)
  - Batches products → saves JSON → data/tiki_products_raw_2026-06-16.json
  - print(".../data/tiki_products_raw_2026-06-16.json")  → XCom
       │
       ▼ Task 3 (Spark container — docker exec)
tiki_load_iceberg.py --raw_file .../tiki_products_raw_2026-06-16.json --layer bronze
  - Spark reads JSON → Bronze Iceberg (overwrite partition)
       │
       ▼ Task 4 (Spark container — docker exec)
tiki_load_iceberg.py --raw_file .../tiki_products_raw_2026-06-16.json --layer silver
  - Detects price changes → Silver price_history (append SCD Type 4)
  - MERGE INTO Silver products (SCD Type 1)
       │
       ▼ Task 5 (Spark container — docker exec)
tiki_gold.py
  - Computes 5 Gold tables (brand_performance, price_trend, etc.)
  - Writes to Iceberg + Reporting Postgres
```

---

## 6. Why use Kafka instead of writing directly to JSON?

| Challenge | Without Kafka (Direct File) | With Kafka |
|--------|---------------|----------|
| Spark job fails midway | Must re-crawl from API entirely | Kafka retains data; consumer simply resumes |
| Multiple downstream pipelines | Must crawl multiple times | Multiple consumer groups can read the same topic independently |
| Separation of Concerns | Extraction + Loading bundled together | Ingestion (Python) is strictly isolated from Processing (Spark) |
| Interview / Defense | "I scraped data and saved a file" | "I implemented Kafka to decouple ingestion and processing, ensuring fault tolerance." |
