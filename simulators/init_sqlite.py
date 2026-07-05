import os
import json
import sqlite3
import glob

def init_db():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(current_dir, "..", "data"))
    db_path = os.path.join(data_dir, "tiki_backend.db")
    
    # Delete old DB if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create products table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        sku TEXT,
        name TEXT,
        url_key TEXT,
        price INTEGER,
        original_price INTEGER,
        discount INTEGER,
        discount_rate INTEGER,
        brand_name TEXT,
        rating_average REAL,
        review_count INTEGER,
        thumbnail_url TEXT,
        category_id INTEGER,
        quantity_sold INTEGER,
        category_name TEXT,
        crawl_date TEXT,
        is_active INTEGER DEFAULT 1
    )
    ''')
    
    mock_dir = os.path.join(data_dir, "mock_data")
    
    if not os.path.exists(mock_dir) or not os.listdir(mock_dir):
        print(f"Error: No crawled JSON files found in {mock_dir}.")
        print("Please run the Tiki crawler first (Step 1) to generate data shards.")
        return

    shard_files = glob.glob(os.path.join(mock_dir, "mock_*.json"))
    if not shard_files:
        print(f"Error: No mock_*.json files found in {mock_dir}.")
        print("Please run the Tiki crawler first to generate data shards.")
        return
    
    total_inserted = 0
    for shard in shard_files:
        print(f"Processing {shard}...")
        with open(shard, "r", encoding="utf-8") as f:
            items = json.load(f)
            
        for p in items:
            # Safely get quantity_sold
            sold = p.get("quantity_sold")
            sold_val = 0
            if isinstance(sold, dict):
                sold_val = sold.get("value", 0)
            elif isinstance(sold, (int, float)):
                sold_val = int(sold)
                
            cursor.execute('''
            INSERT OR REPLACE INTO products (
                id, sku, name, url_key, price, original_price, discount, discount_rate, 
                brand_name, rating_average, review_count, thumbnail_url, category_id, 
                quantity_sold, category_name, crawl_date, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                p.get("id"), p.get("sku"), p.get("name"), p.get("url_key"), 
                p.get("price", 0), p.get("original_price", 0), p.get("discount", 0), 
                p.get("discount_rate", 0), p.get("brand_name", ""), 
                p.get("rating_average", 0), p.get("review_count", 0), 
                p.get("thumbnail_url", ""), p.get("category_id"), sold_val, 
                p.get("category_name", ""), p.get("crawl_date", ""), 1
            ))
        
        total_inserted += len(items)
        conn.commit()
        
    print(f"Successfully inserted {total_inserted} products into {db_path}!")
    
    # Create Index for fast querying
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_category_id ON products(category_id)')
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
