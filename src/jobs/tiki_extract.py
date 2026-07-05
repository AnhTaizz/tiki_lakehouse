import argparse
import json
import os
import sys
from datetime import datetime

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


def crawl_tiki_data(target_category_id, target_category_name, logical_date_str=None, logical_timestamp_str=None):
    logger.info("Start crawling category id %s", target_category_id)

    # --- CIRCUIT BREAKER LOGIC (HOURLY) ---
    if logical_timestamp_str:
        try:
            from datetime import timezone
            logical_time = datetime.fromisoformat(logical_timestamp_str)
            # Ensure physical time has timezone (UTC) to compute accurate diff
            physical_time = datetime.now(timezone.utc)

            diff_hours = (physical_time - logical_time).total_seconds() / 3600.0

            if diff_hours > 8:
                logger.warning(f"CIRCUIT BREAKER TRIGGERED: Logical Time ({logical_time}) is {diff_hours:.1f} hours behind Physical Time.")
                logger.warning("Skipping extraction to prevent Time-Travel Data Pollution in the Lakehouse.")
                sys.exit(99) # 99 tells Airflow BashOperator to mark the task as SKIPPED
            elif diff_hours < -1:
                logger.warning(f"CIRCUIT BREAKER TRIGGERED: Logical Time ({logical_time}) is in the future.")
                sys.exit(99)
            else:
                logger.info(f"Circuit Breaker PASSED: Time diff is {diff_hours:.1f} hours (Safe threshold <= 8h).")
        except Exception as e:
            logger.error(f"Failed to parse logical_timestamp: {e}")
    # --------------------------------------

    logger.info("==================================================")
    logger.info("AIRFLOW TASK START: EXTRACT_AND_PUBLISH")
    logger.info("Target Category : %s (ID: %s)", target_category_name, target_category_id)
    logger.info("Target Kafka    : %s", KAFKA_TOPIC)
    logger.info("==================================================")

    try:
        logger.info("Initiating concurrent API extraction for category '%s'...", target_category_name)
        products = fetch_products_by_category(target_category_id, "")
        
        # Inject category metadata
        for p in products:
            p["category_id"] = target_category_id
            p["category_name"] = target_category_name
            
        logger.info("Successfully extracted %d products from API.", len(products))
        
    except Exception as e:
        logger.error("Failed to extract category %s: %s", target_category_name, e)
        sys.exit(1)

    # Use logical date if provided, otherwise fallback to today (physical date)
    crawl_date = logical_date_str if logical_date_str else datetime.now().strftime("%Y-%m-%d")

    if products:
        logger.info("Publishing %d products to Kafka...", len(products))
        publish_to_kafka(products, crawl_date)
    else:
        logger.warning("No products found for category %s. Nothing to publish.", target_category_name)

    logger.info("==================================================")
    logger.info("TASK COMPLETED: Extracted and published %d products.", len(products))
    logger.info("==================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tiki Data Crawler")
    parser.add_argument("--category_id", type=int, required=True, help="Category ID to crawl")
    parser.add_argument("--category_name", type=str, required=True, help="Parent Category Name")
    parser.add_argument("--logical_date", type=str, required=False, help="Airflow Execution Date (YYYY-MM-DD) for Partitioning")
    parser.add_argument("--logical_timestamp", type=str, required=False, help="Airflow Execution Timestamp (ISO8601) for Circuit Breaker")
    args = parser.parse_args()
    crawl_tiki_data(args.category_id, args.category_name, args.logical_date, args.logical_timestamp)
