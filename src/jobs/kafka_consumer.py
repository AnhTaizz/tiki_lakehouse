"""
kafka_consumer.py
=================
Consume all products from Kafka topic 'tiki.raw.products',
collect them into a single JSON file under data/, then print the file path to stdout
so Airflow XCom can pass it to the next task (tiki_load_iceberg.py on Spark).

Runs inside the Airflow container — no PySpark required.
"""


import argparse
import json
import os
import sys

from kafka import KafkaConsumer

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, ".."))
project_dir = os.path.abspath(os.path.join(src_dir, ".."))

if src_dir not in sys.path:
    sys.path.append(src_dir)

from common.utils import save_to_json, setup_logger

logger = setup_logger(__name__)

KAFKA_BROKER        = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC         = "tiki.raw.products"
CONSUMER_GROUP      = "tiki-bronze-loader"
# Stop consuming after 30s of no new messages — enough time to drain one crawl batch
CONSUMER_TIMEOUT_MS = 30_000


def consume_and_save(crawl_date: str) -> str:
    """
    Consume all messages from the topic for today's crawl_date,
    save them as a JSON file, and return the absolute path for the Spark job.
    """
    logger.info(
        "Connecting to Kafka broker '%s', topic '%s', group '%s'",
        KAFKA_BROKER, KAFKA_TOPIC, CONSUMER_GROUP,
    )

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",       # read from the beginning of the topic (idempotent re-run)
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,  # stop automatically when no new messages
    )

    products = []
    for message in consumer:
        products.append(message.value)

    consumer.close()
    logger.info("Consumed %d products from topic '%s'", len(products), KAFKA_TOPIC)

    if not products:
        logger.error("No products consumed! Kafka topic may be empty. Aborting.")
        sys.exit(1)

    raw_filename = f"tiki_products_raw_{crawl_date}.json"
    raw_filepath = os.path.join(project_dir, "data", raw_filename)
    save_to_json(products, raw_filepath)
    logger.info("Saved %d products to %s", len(products), raw_filepath)

    # Airflow XCom captures it — Task 3 (Spark) will use
    print(raw_filepath)
    return raw_filepath


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka consumer — Tiki Lakehouse")
    parser.add_argument(
        "--crawl_date",
        required=True,
        help="Crawl date in YYYY-MM-DD format, used to name the output file.",
    )
    args = parser.parse_args()
    consume_and_save(args.crawl_date)
