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
        import math
        while True:
            try:
                # Use Sine wave to generate simulated traffic waves (Cycle ~60 seconds)
                # The wave will smoothly transition from a low base (50) to a massive peak (1500)
                current_time = time.time()
                wave = (math.sin(current_time / 10.0) + 1) / 2 # Normalize to 0.0 -> 1.0
                
                base_traffic = 50
                peak_traffic = 1500
                batch_size = int(base_traffic + (peak_traffic * wave))
                batch_size += random.randint(-30, 30) # Add a little noise for realism
                batch_size = max(10, batch_size) # Ensure no negative batch sizes

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

                conn.commit()
                producer.flush()
                logger.info("Tick: Sent %d events (Flash Sale: %d, Purchase: %d, Hide: %d, Restock: %d) to Kafka. Waiting...",
                            len(rows), flash_sales, purchases, unpublished, restocked)

                # Sleep briefly (1 to 3s) to simulate constant high throughput
                time.sleep(random.uniform(1.0, 3.0))

            except KeyboardInterrupt:
                logger.info("Simulator stopped by user.")
                break
            except Exception as e:
                logger.warning("Simulator warning (e.g. SQLite DB locked). Retrying in 2 seconds...: %s", e)
                time.sleep(2)
    finally:
        if producer:
            producer.close()
        conn.close()

if __name__ == "__main__":
    run_continuous_simulator()
