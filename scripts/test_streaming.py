import json
import logging
import os
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092") # Run on host machine
KAFKA_TOPIC = "tiki.stream.products"

def run_test():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8")
        )
        logger.info(f"Connected to Kafka: {KAFKA_BROKER}")

        test_event = {
            "id": 999999999,
            "name": "Streaming Test Product",
            "price": 500000,
            "original_price": 1000000,
            "discount": 500000,
            "discount_rate": 50,
            "quantity_sold": 100,
            "category_id": 1520,
            "crawl_date": datetime.now().strftime("%Y-%m-%d"),
            "_event_type": "TEST_EVENT" # Mark this as a test event
        }

        logger.info(f"Sending test event to topic '{KAFKA_TOPIC}'...")
        producer.send(KAFKA_TOPIC, key=str(test_event["id"]).encode("utf-8"), value=test_event)
        producer.flush()

        logger.info(" Successfully sent!")
        logger.info(" Please wait for ~10 seconds (Spark Streaming trigger interval)...")
        logger.info(" Refresh the Superset dashboard, you should see a 'TEST_EVENT' column appear!")

    except Exception as e:
        logger.error(f"Failed to send test event: {e}")

if __name__ == "__main__":
    run_test()
