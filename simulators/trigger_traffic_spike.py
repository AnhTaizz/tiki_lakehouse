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

def run_traffic_spike():
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

    logger.info("Starting MASSIVE TRAFFIC SPIKE simulation (Duration: 3 minutes)...")

    # Giả lập 6 đợt sóng liên tiếp, mỗi đợt cách nhau 30 giây
    # Sẽ duy trì cái cột biểu đồ ở mức cao chót vót trong suốt 3 phút.
    import time
    total_events = 0
    for wave in range(6):
        logger.info("Triggering Wave %d/6 of Massive Traffic...", wave + 1)
        # Giảm số lượng sản phẩm xuống 1000 để giảm tải cho máy cá nhân
        cursor.execute("SELECT * FROM products WHERE is_active=1 ORDER BY RANDOM() LIMIT 1000")
        rows = cursor.fetchall()

        # Cập nhật số lượng bán vào DB thật nhanh trước
        db_updates = []
        for row in rows:
            p = dict(row)
            new_qty = int(p.get("quantity_sold", 0)) + random.randint(1, 10)
            db_updates.append((new_qty, p["id"]))
        
        cursor.executemany("UPDATE products SET quantity_sold=? WHERE id=?", db_updates)
        conn.commit()

        # Sau khi nhả DB ra (không làm lock DB nữa), đẩy 10.000 sự kiện lên Kafka
        events_sent = 0
        for i, row in enumerate(rows):
            p = dict(row)
            vn_tz = timezone(timedelta(hours=7))
            p["crawl_date"] = datetime.now(vn_tz).strftime("%Y-%m-%d")

            # Bắn tổng cộng 10 events / sản phẩm: 4 PING, 3 PURCHASE, 3 FLASH_SALE
            for _ in range(4):
                p_ping = p.copy()
                p_ping["_event_type"] = "PING"
                producer.send(KAFKA_TOPIC, key=str(p_ping["id"]).encode("utf-8"), value=p_ping)
                events_sent += 1
                
            for _ in range(3):
                p_purchase = p.copy()
                p_purchase["_event_type"] = "PURCHASE"
                producer.send(KAFKA_TOPIC, key=str(p_purchase["id"]).encode("utf-8"), value=p_purchase)
                
                p_flash = p.copy()
                p_flash["_event_type"] = "FLASH_SALE"
                producer.send(KAFKA_TOPIC, key=str(p_flash["id"]).encode("utf-8"), value=p_flash)
                
                events_sent += 2

            if i % 250 == 0:
                producer.flush()

        producer.flush()
        total_events += events_sent
        logger.info("Wave %d completed: Sent %d events. Maintaining chaos...", wave + 1, events_sent)
        
        if wave < 5:
            time.sleep(30)

    conn.close()
    logger.info("Successfully published a total of %d TRAFFIC SPIKE events over 3 minutes.", total_events)

if __name__ == "__main__":
    run_traffic_spike()
