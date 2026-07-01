import json
import logging
import os
import random
import sys
import time
import sqlite3
from datetime import datetime

from kafka import KafkaProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC  = "tiki.stream.products" # Use separate topic for streaming to avoid interfering with batch
SOURCE_FILE  = "mock_data/mock_1520.json" # Share the same mock dataset with Batch to ensure synchronization

def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"),
            retries=3,
        )
        return producer
    except Exception as e:
        logger.error("Failed to connect to Kafka at %s: %s", KAFKA_BROKER, e)
        return None

def get_db_connection():
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
    db_path = os.path.join(data_dir, "tiki_backend.db")
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except:
        pass
    conn.row_factory = sqlite3.Row
    return conn

def run_continuous_simulator():
    producer = get_kafka_producer()
    if not producer:
        return

    logger.info("🚀 TIKI CONTINUOUS STREAMING SIMULATOR STARTED!")
    logger.info("Press Ctrl+C to stop. Emitting events to topic: %s", KAFKA_TOPIC)

    conn = get_db_connection()

    try:
        while True:
            # Fetch 5 to 20 random products across ALL categories
            batch_size = random.randint(5, 20)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM products ORDER BY RANDOM() LIMIT ?", (batch_size,))
            rows = cursor.fetchall()

            flash_sales = 0
            purchases = 0
            unpublished = 0
            restocked = 0

            for row in rows:
                p = dict(row)
                dice = random.random()

                is_active_val = p.get("is_active", 1)

                if is_active_val == 1:
                    if dice < 0.03:
                        p["is_active"] = False
                        p["_event_type"] = "UNPUBLISHED"
                        unpublished += 1
                        cursor.execute("UPDATE products SET is_active=0 WHERE id=?", (p["id"],))
                    elif dice < 0.20:
                        # Flash Sale
                        p["is_active"] = True
                        discount_percent = random.uniform(0.05, 0.15)
                        current_price = p.get("price", 0)
                        if current_price > 0:
                            new_price = int(current_price * (1 - discount_percent))
                            original_price = p.get("original_price", current_price)
                            p["price"] = new_price
                            p["discount"] = original_price - new_price
                            p["discount_rate"] = int((p["discount"] / original_price) * 100) if original_price > 0 else 0
                            p["_event_type"] = "FLASH_SALE"
                            flash_sales += 1
                            cursor.execute("""
                                UPDATE products SET price=?, discount=?, discount_rate=? WHERE id=?
                            """, (p["price"], p["discount"], p["discount_rate"], p["id"]))
                    elif dice < 0.60:
                        # Purchase
                        p["is_active"] = True
                        sold_increase = random.randint(1, 10)
                        p["quantity_sold"] = int(p.get("quantity_sold", 0)) + sold_increase
                        p["_event_type"] = "PURCHASE"
                        purchases += 1
                        cursor.execute("""
                            UPDATE products SET quantity_sold=? WHERE id=?
                        """, (p["quantity_sold"], p["id"]))
                    else:
                        p["is_active"] = True
                        p["_event_type"] = "PING"
                else:
                    # Inactive product (is_active = 0)
                    if dice < 0.20: # 20% chance to be restocked
                        p["is_active"] = True
                        p["_event_type"] = "RESTOCK"
                        restocked += 1
                        cursor.execute("UPDATE products SET is_active=1 WHERE id=?", (p["id"],))
                    else:
                        # Remain inactive, emit no event
                        continue

                # Attach current date
                p["crawl_date"] = datetime.now().strftime("%Y-%m-%d")

                # Emit Event to Kafka Stream
                producer.send(
                    KAFKA_TOPIC,
                    key=str(p["id"]).encode("utf-8"),
                    value=p
                )

            try:
                conn.commit()
            except sqlite3.Error as e:
                logger.warning("SQLite disk I/O locked (Docker volume issue) - Ignoring: %s", e)

            producer.flush()
            logger.info("🕒 Tick: Sent %d events (Flash Sale: %d, Purchase: %d, Hide: %d, Restock: %d) to Kafka. Waiting...",
                        len(rows), flash_sales, purchases, unpublished, restocked)

            # Sleep 2-5 seconds before next tick
            time.sleep(random.uniform(2.0, 5.0))

    except KeyboardInterrupt:
        logger.info("🛑 Simulator stopped by user.")
    except Exception as e:
        logger.error("❌ Simulator error: %s", e)
    finally:
        if producer:
            producer.close()
        conn.close()

if __name__ == "__main__":
    run_continuous_simulator()
