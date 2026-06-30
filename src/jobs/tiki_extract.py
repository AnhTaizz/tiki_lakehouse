import argparse
import json
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from kafka import KafkaProducer

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, ".."))
project_dir = os.path.abspath(os.path.join(src_dir, ".."))

if src_dir not in sys.path:
    sys.path.append(src_dir)

from common.tiki_category import load_categories_from_api, load_categories_from_file, get_leaf_categories
from common.tiki_product import fetch_products_by_category
from common.utils import setup_logger


logger = setup_logger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC  = "tiki.raw.products"


def publish_to_kafka(products: list, crawl_date: str) -> None:
    """Publish each product as a message to the Kafka topic tiki.raw.products."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",   # wait for broker acknowledgment before returning
        retries=3,
    )

    sent = 0
    for product in products:
        product["crawl_date"] = crawl_date   # inject crawl metadata into each message
        producer.send(KAFKA_TOPIC, key=str(product["id"]).encode("utf-8"), value=product)
        sent += 1

    producer.flush()   # ensure all messages are delivered before closing
    producer.close()
    logger.info("Published %d products to Kafka topic '%s'", sent, KAFKA_TOPIC)


def crawl_tiki_data(target_category_id, target_category_name):
    logger.info("Start crawling category id %s", target_category_id)

    leaf_categories = [{"id": target_category_id, "name": target_category_name, "url_key": ""}]
    logger.info("Using Root Category directly for extraction: %s", target_category_id)

    all_products = []

    def process_category(category, index, total):
        logger.info("[%s/%s] Processing: %s", index + 1, total, category["name"])
        try:
            products = fetch_products_by_category(category["id"], category["url_key"])
            for p in products:
                p["category_id"] = target_category_id
                p["category_name"] = target_category_name
            logger.info("[%s/%s] FINISHED: %s - Extracted %d products", index + 1, total, category["name"], len(products))

            return products
        except Exception as e:
            logger.error("Error fetching category %s: %s", category["name"], e)
            return []

    crawl_date = datetime.now().strftime("%Y-%m-%d")
    total_crawled = 0

    logger.info("Starting extraction with 3 workers (Mock API Boost - Safe Mode)...")
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(process_category, cat, idx, len(leaf_categories))
            for idx, cat in enumerate(leaf_categories)
        ]

        for future in as_completed(futures):
            products = future.result()
            if products:
                publish_to_kafka(products, crawl_date)
                total_crawled += len(products)

    logger.info("Finished crawling. Total products published to Kafka: %d", total_crawled)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tiki Data Crawler")
    parser.add_argument("--category_id", type=int, required=True, help="Category ID to crawl")
    parser.add_argument("--category_name", type=str, required=True, help="Parent Category Name")
    args = parser.parse_args()
    crawl_tiki_data(args.category_id, args.category_name)
