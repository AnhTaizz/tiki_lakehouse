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

def run_out_of_stock():
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
    
    logger.info("Starting Massive Out-Of-Stock simulation...")
    
    # Láº¥y 100 sáº£n pháº©m Ä‘ang bÃ¡n cháº¡y (quantity_sold > 100) Ä‘á»ƒ mÃ´ phá»ng bá»‹ cÃ n quÃ©t kho
    cursor.execute("SELECT * FROM products WHERE is_active=1 AND quantity_sold > 100 ORDER BY RANDOM() LIMIT 100")
    rows = cursor.fetchall()
    
    events_sent = 0
    for row in rows:
        p = dict(row)
        
        # ChÃ¡y hÃ ng: is_active = 0
        p["is_active"] = 0
        p["_event_type"] = "MASSIVE_OUT_OF_STOCK"
        
        vn_tz = timezone(timedelta(hours=7))
        p["crawl_date"] = datetime.now(vn_tz).strftime("%Y-%m-%d")
        
        # Cáº­p nháº­t DB
        cursor.execute("UPDATE products SET is_active=0 WHERE id=?", (p["id"],))
        
        # Báº¯n cáº£nh bÃ¡o
        producer.send(KAFKA_TOPIC, key=str(p["id"]).encode("utf-8"), value=p)
        events_sent += 1

    conn.commit()
    producer.flush()
    conn.close()
    
    logger.info("Successfully published %d MASSIVE_OUT_OF_STOCK events to Kafka.", events_sent)

if __name__ == "__main__":
    run_out_of_stock()
