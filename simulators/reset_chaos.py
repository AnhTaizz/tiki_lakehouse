import os
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def reset_chaos():
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    db_path = os.path.join(data_dir, "tiki_backend.db")

    if not os.path.exists(db_path):
        logger.error("Database not found! Run Init DB first.")
        return

    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except:
        pass
    cursor = conn.cursor()

    logger.info("Initializing System Reset...")

    try:
        cursor.execute("""
            UPDATE products
            SET price = original_price,
                discount = 0,
                discount_rate = 0
            WHERE discount_rate >= 85
        """)
        rows_affected = cursor.rowcount

        cursor.execute("""
            UPDATE products
            SET is_active = 1
            WHERE is_active = 0
        """)
        rows_affected += cursor.rowcount

        conn.commit()

        logger.info("==================================================")
        logger.info("CHAOS RESET SUCCESSFUL!")
        logger.info(f"Restored {rows_affected} products back to normal state.")
        logger.info("- Prices reverted to Original Price.")
        logger.info("- Inventory unlocked and republished.")
        logger.info("The Continuous Simulator will now broadcast normal data.")
        logger.info("==================================================")

    except Exception as e:
        logger.error(f"Failed to reset database: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    reset_chaos()
