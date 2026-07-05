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

def run_viral_trend():
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
    
    logger.info("Starting Viral Trend simulation...")
    
    # Láº¥y 5 sáº£n pháº©m ngáº«u nhiÃªn Ä‘á»ƒ buff doanh sá»‘ 
    cursor.execute("SELECT * FROM products WHERE is_active=1 ORDER BY RANDOM() LIMIT 5")
    rows = cursor.fetchall()
    
    events_sent = 0
    for row in rows:
        p = dict(row)
        
        # BÃ£o Ä‘Æ¡n: TÄƒng vá»t 5,000 - 15,000 lÆ°á»£t bÃ¡n chá»‰ trong tÃ­ch táº¯c
        surge_sales = random.randint(5000, 15000)
        p["quantity_sold"] = int(p.get("quantity_sold", 0)) + surge_sales
        p["_event_type"] = "VIRAL_SURGE"
        
        vn_tz = timezone(timedelta(hours=7))
        p["crawl_date"] = datetime.now(vn_tz).strftime("%Y-%m-%d")
        
        # Cáº­p nháº­t DB
        cursor.execute("""
            UPDATE products SET quantity_sold=? WHERE id=?
        """, (p["quantity_sold"], p["id"]))
        
        # Báº¯n cáº£nh bÃ¡o vÃ o Kafka
        producer.send(KAFKA_TOPIC, key=str(p["id"]).encode("utf-8"), value=p)
        events_sent += 1
        
        logger.info("Viral surge triggered on product_id=%s. Added sales: %d", p["id"], surge_sales)

    conn.commit()
    producer.flush()
    conn.close()
    
    logger.info("Successfully published %d VIRAL_SURGE events to Kafka.", events_sent)

if __name__ == "__main__":
    run_viral_trend()
