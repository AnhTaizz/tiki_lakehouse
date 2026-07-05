import json
import logging
import os
import sqlite3
import random
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9093")
KAFKA_TOPIC  = "tiki.stream.products"

def run_mega_live():
    logger.info("Connecting to Kafka at %s...", KAFKA_BROKER)
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"),
        )
    except Exception as e:
        logger.error("Could not connect to Kafka: %s", e)
        return

    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    db_path = os.path.join(data_dir, "tiki_backend.db")
    logger.info("Connecting to Mock DB: %s...", db_path)

    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except:
        pass
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    logger.info("Starting Mega Flash Sale simulation...")

    # Select 5000 random active products
    cursor.execute("SELECT * FROM products WHERE is_active=1 ORDER BY RANDOM() LIMIT 5000")
    rows = cursor.fetchall()

    events_sent = 0
    for row in rows:
        p = dict(row)
        original_price = p.get("original_price") or p.get("price") or 100000
        if original_price <= 0:
            original_price = 100000

        # Slash price dramatically (30% to 70% off)
        discount_percent = random.uniform(0.3, 0.7)
        new_price = int(original_price * (1 - discount_percent))

        p["price"] = new_price
        p["discount"] = original_price - new_price
        p["discount_rate"] = int(discount_percent * 100)

        # Surge in sales (buy 50 to 300 units instantly)
        p["quantity_sold"] = int(p.get("quantity_sold", 0)) + random.randint(50, 300)
        p["_event_type"] = "MEGA_FLASH_SALE"

        vn_tz = timezone(timedelta(hours=7))
        p["crawl_date"] = datetime.now(vn_tz).strftime("%Y-%m-%d")

        # Update DB so the batch pipeline sees it later
        cursor.execute("""
            UPDATE products SET price=?, discount=?, discount_rate=?, quantity_sold=? WHERE id=?
        """, (p["price"], p["discount"], p["discount_rate"], p["quantity_sold"], p["id"]))

        # Emit to Kafka for Instant Streaming Update
        producer.send(KAFKA_TOPIC, key=str(p["id"]).encode("utf-8"), value=p)
        events_sent += 1

    conn.commit()
    producer.flush()
    conn.close()

    logger.info("Successfully published %d MEGA_FLASH_SALE events to Kafka.", events_sent)

if __name__ == "__main__":
    run_mega_live()
