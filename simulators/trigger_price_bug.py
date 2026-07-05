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

def run_price_bug():
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
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except:
        pass
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    logger.info("Starting Fatal Price Bug simulation...")
    
    # Select 10 expensive products (price > 1,000,000) to make the impact shocking
    cursor.execute("SELECT * FROM products WHERE is_active=1 AND price > 1000000 ORDER BY RANDOM() LIMIT 10")
    rows = cursor.fetchall()
    
    events_sent = 0
    for row in rows:
        p = dict(row)
        original_price = p.get("original_price") or p.get("price") or 5000000
        
        # Price Bug: Drop by 90% - 98% (e.g. 20M VND laptop drops to 500k VND)
        discount_percent = random.uniform(0.90, 0.98)
        new_price = int(original_price * (1 - discount_percent))
        
        p["price"] = new_price
        p["discount"] = original_price - new_price
        p["discount_rate"] = int(discount_percent * 100)
        
        p["_event_type"] = "FATAL_PRICE_DROP"
        
        vn_tz = timezone(timedelta(hours=7))
        p["crawl_date"] = datetime.now(vn_tz).strftime("%Y-%m-%d")
        
        # Update DB
        cursor.execute("""
            UPDATE products SET price=?, discount=?, discount_rate=? WHERE id=?
        """, (p["price"], p["discount"], p["discount_rate"], p["id"]))
        
        # Fire alert immediately to Kafka
        producer.send(KAFKA_TOPIC, key=str(p["id"]).encode("utf-8"), value=p)
        events_sent += 1
        
        logger.warning("Price Bug triggered on product_id=%s. Original: %d, New: %d", p["id"], original_price, new_price)

    conn.commit()
    producer.flush()
    conn.close()
    
    logger.info("Successfully published %d FATAL_PRICE_DROP events to Kafka.", events_sent)

if __name__ == "__main__":
    run_price_bug()
