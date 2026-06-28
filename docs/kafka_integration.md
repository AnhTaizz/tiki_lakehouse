# Kafka Integration — Tài liệu học tập

## 1. Tổng quan

Dự án tích hợp **Apache Kafka** vào pipeline Tiki Lakehouse nhằm tách biệt rõ ràng hai giai đoạn:

- **Ingestion** (thu thập dữ liệu): Python + Kafka
- **Processing** (xử lý dữ liệu): Apache Spark

Pattern này được gọi là **Hybrid Architecture** — rất phổ biến trong các hệ thống Data Engineering thực tế.

---

## 2. Kiến trúc trước và sau khi thêm Kafka

### Trước (Batch Pipeline)
```
[Airflow - 1 lần/ngày]
  Task 1: tiki_extract.py  →  lưu file JSON vào data/
  Task 2: tiki_load_iceberg.py  →  Spark đọc JSON → Bronze → Silver
  Task 3: tiki_gold.py  →  Spark tính Gold → Superset
```

### Sau (Batch Pipeline với Kafka - mỗi 4 tiếng)
```
[Airflow - mỗi 4 tiếng]
  Task 1: tiki_extract.py (Producer)  →  crawl API → publish từng product lên Kafka topic
  Task 2: kafka_consumer.py (Consumer)  →  consume Kafka → gom lại → lưu file JSON
  Task 3: tiki_load_iceberg.py  →  Spark đọc JSON → Bronze → Silver  [GIỮ NGUYÊN]
  Task 4: tiki_gold.py  →  Spark tính Gold → Superset               [GIỮ NGUYÊN]
```

---

## 3. Các khái niệm Kafka cốt lõi trong dự án

### 3.1 Topic
Hàng đợi chứa dữ liệu. Giống một "channel" — producer gửi vào, consumer đọc ra.

```
Topic name: tiki.raw.products

Nội dung (mỗi message là 1 product):
  offset 0  → {"id": 123, "name": "Kem chống nắng", "price": 250000, "crawl_date": "2026-06-16", ...}
  offset 1  → {"id": 124, "name": "Son môi", "price": 180000, ...}
  ...
  offset 4999 → {...}
```

### 3.2 Producer
Bên **gửi** dữ liệu vào Kafka. Trong dự án này là `tiki_extract.py`.

```python
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",   # địa chỉ Kafka broker
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # chuyển dict → bytes
    acks="all",    # broker phải xác nhận ghi thành công
    retries=3,     # tự retry nếu lỗi
)
producer.send("tiki.raw.products", value=product_dict)
producer.flush()   # đảm bảo tất cả message đã lên broker
producer.close()
```

### 3.3 Consumer
Bên **đọc** dữ liệu từ Kafka. Trong dự án này là `kafka_consumer.py`.

```python
consumer = KafkaConsumer(
    "tiki.raw.products",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",    # đọc từ đầu topic (idempotent khi re-run)
    group_id="tiki-bronze-loader",   # Kafka track offset theo group_id này
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),  # bytes → dict
    consumer_timeout_ms=30_000,      # tự dừng sau 30s không có message mới
)

for message in consumer:
    product = message.value   # lấy nội dung message
```

### 3.4 Consumer Group
`group_id="tiki-bronze-loader"` — Kafka dùng ID này để **ghi nhớ offset** đã đọc đến đâu. Nếu consumer bị crash rồi chạy lại, nó biết tiếp tục từ chỗ nào thay vì đọc lại từ đầu.

---

## 4. Các file thay đổi

### 4.1 `requirement.txt` — Thêm thư viện Kafka
```diff
+ kafka-python==2.0.2
```

### 4.2 `docker-compose.yml` — Thêm 2 service mới

```yaml
zookeeper:        # Quản lý cluster Kafka (bắt buộc đi kèm Kafka)
  image: confluentinc/cp-zookeeper:7.6.1
  environment:
    ZOOKEEPER_CLIENT_PORT: 2181

kafka:            # Kafka broker — nơi lưu trữ topics và messages
  image: confluentinc/cp-kafka:7.6.1
  depends_on: [zookeeper]
  ports: ["9092:9092"]
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092   # địa chỉ nội bộ Docker
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"              # tự tạo topic khi producer gửi lần đầu
```

Ngoài ra thêm `KAFKA_BROKER: kafka:9092` vào environment của `airflow-scheduler` để các job Python biết địa chỉ Kafka.

### 4.3 `src/jobs/tiki_extract.py` — Chuyển thành Kafka Producer

| Trước | Sau |
|-------|-----|
| Crawl xong → `save_to_json(all_products, filepath)` | Crawl xong → `publish_to_kafka(all_products, crawl_date)` |
| `print(raw_filepath)` → XCom push filepath | `print(crawl_date)` → XCom push ngày crawl |
| Trả về string filepath | Trả về string crawl_date |

### 4.4 `src/jobs/kafka_consumer.py` — File mới hoàn toàn

- Nhận `--crawl_date` từ Airflow XCom (qua argument)
- Consume toàn bộ messages từ topic `tiki.raw.products`
- Gom lại thành list, lưu ra `data/tiki_products_raw_YYYY-MM-DD.json`
- `print(raw_filepath)` → XCom push cho Task 3 (Spark)

### 4.5 `dags/tiki_pipeline_dag.py` — Thêm Task 2, sửa schedule

```diff
- schedule_interval="0 15 * * *"       # 1 lần/ngày 15:00
+ schedule_interval="0 1,5,9,13,17,21 * * *"  # mỗi 4 tiếng (8h, 12h, 16h, 20h, 0h, 4h ICT)
```

**4 Task thay vì 3:**
```
Task 1: extract_and_publish   ← đổi tên, đổi logic (Producer)
Task 2: consume_from_kafka    ← MỚI HOÀN TOÀN (Consumer)
Task 3: load_bronze_silver    ← giữ nguyên 100%
Task 4: transform_gold        ← giữ nguyên 100%
```

**XCom chain:**
```
Task 1 → print(crawl_date)   "2026-06-16"
Task 2 → ti.xcom_pull(extract_and_publish) → nhận crawl_date → print(filepath)
Task 3 → ti.xcom_pull(consume_from_kafka) → nhận filepath → chạy Spark
```

---

## 5. Luồng dữ liệu chi tiết (end-to-end)

```
[08:00 ICT — Airflow trigger]
       │
       ▼ Task 1 (Airflow container)
tiki_extract.py
  - Gọi Tiki API, crawl sản phẩm từ 5 ngành hàng chính
  - Với mỗi sản phẩm → producer.send("tiki.raw.products", product)
  - flush() → close()
  - print("2026-06-16")  → XCom
       │
       ▼ Task 2 (Airflow container)
kafka_consumer.py --crawl_date "2026-06-16"
  - Kết nối Kafka broker tại kafka:9092
  - Đọc hết messages từ topic "tiki.raw.products" (dừng sau 30s idle)
  - Gom products → save JSON → data/tiki_products_raw_2026-06-16.json
  - print("/opt/airflow/data/tiki_products_raw_2026-06-16.json")  → XCom
       │
       ▼ Task 3 (Spark container — docker exec)
tiki_load_iceberg.py --raw_file .../tiki_products_raw_2026-06-16.json
  - Spark đọc JSON → Bronze Iceberg (overwrite partition)
  - Detect price changes → Silver price_history (append)
  - MERGE INTO Silver products (SCD Type 1)
       │
       ▼ Task 4 (Spark container — docker exec)
tiki_gold.py
  - Tính 5 Gold tables: brand_performance, price_trend, discount_analysis, top_products, daily_summary
  - Ghi ra Iceberg + Reporting Postgres → Superset dashboard cập nhật
```

---

## 6. Tại sao dùng Kafka thay vì ghi thẳng vào file?

| Vấn đề | Không có Kafka | Có Kafka |
|--------|---------------|----------|
| Spark bị lỗi giữa chừng | Phải crawl lại từ đầu | Kafka vẫn giữ data, consumer chạy lại là xong |
| Nhiều downstream cùng cần data | Phải crawl nhiều lần | Nhiều consumer group đọc độc lập cùng 1 topic |
| Tách biệt trách nhiệm | Extract + Load gộp làm 1 | Ingestion (Python) độc lập với Processing (Spark) |
| Giải thích khi phỏng vấn | "Tôi crawl rồi lưu file" | "Tôi dùng Kafka để decouple ingestion và processing" |

---

### Q: Dự án này có phải là Real-time streaming không?
> *"Không, dự án này xử lý theo mẻ (batch processing) với chu kỳ 4 tiếng một lần. Việc thêm Kafka đóng vai trò như một Message Broker để decouple (tách rời) việc thu thập dữ liệu (crawl) và xử lý (Spark). Dữ liệu sau khi crawl được đẩy vào Kafka và consumer sẽ gom mẻ lại (batch) để lưu trữ. Điều này giúp pipeline đáng tin cậy hơn, chịu lỗi tốt hơn, đồng thời là tiền đề cho kiến trúc near-real-time nếu sau này có webhook."*
