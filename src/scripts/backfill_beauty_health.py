import os
import re
import json
import logging
from datetime import datetime
from kafka import KafkaProducer

"""
SCRIPT: BACKFILL HISTORICAL DATA (HỒI PHỤC DỮ LIỆU LỊCH SỬ)
Mục đích: Đọc các file JSON raw lịch sử của danh mục 'Làm Đẹp - Sức Khỏe' đã cào từ tháng 5, tháng 6.
Sau đó bơm (publish) lại toàn bộ vào Kafka (tiki.raw.products) để Consumer nạp vào Iceberg.
Cách chạy:
docker exec airflow_scheduler python /opt/airflow/src/scripts/backfill_beauty_health.py
"""

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "tiki.raw.products"

def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            retries=3,
        )
        return producer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka at {KAFKA_BROKER}: {e}")
        return None

def backfill_historical_data():
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
    if not os.path.exists(data_dir):
        logger.error(f"Data directory not found: {data_dir}")
        return

    producer = get_kafka_producer()
    if not producer:
        return

    # Find all tiki_beauty_health_raw_YYYY-MM-DD.json files
    pattern = re.compile(r"tiki_beauty_health_raw_(\d{4}-\d{2}-\d{2})\.json")

    total_published = 0

    for filename in sorted(os.listdir(data_dir)):
        match = pattern.match(filename)
        if match:
            crawl_date = match.group(1)
            filepath = os.path.join(data_dir, filename)

            logger.info(f"Processing historical file: {filename} (Date: {crawl_date})")

            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if not isinstance(data, list):
                    logger.warning(f"File {filename} does not contain a JSON array. Skipping.")
                    continue

                # Add crawl_date if missing and publish
                count = 0
                for item in data:
                    if "crawl_date" not in item:
                        item["crawl_date"] = crawl_date

                    producer.send(KAFKA_TOPIC, value=item)
                    count += 1

                producer.flush()
                logger.info(f"✅ Published {count} products from {crawl_date} to Kafka topic '{KAFKA_TOPIC}'")
                total_published += count

            except Exception as e:
                logger.error(f"Failed to process file {filename}: {e}")

    logger.info(f"Backfill complete! Total products published: {total_published}")

if __name__ == "__main__":
    backfill_historical_data()
