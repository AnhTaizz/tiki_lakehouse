import os
import json
import sqlite3
import glob

def init_db():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(current_dir, "..", "..", "data"))
    db_path = os.path.join(data_dir, "tiki_backend.db")
    
    # Xóa DB cũ nếu tồn tại
    if os.path.exists(db_path):
        os.remove(db_path)
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Tạo bảng products
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
        crawl_date TEXT
    )
    ''')
    
    # Đọc các file JSON shard và insert vào DB
    mock_dir = os.path.join(data_dir, "mock_data")
    shard_files = glob.glob(os.path.join(mock_dir, "mock_*.json"))
    
    total_inserted = 0
    for shard in shard_files:
        print(f"Processing {shard}...")
        with open(shard, "r", encoding="utf-8") as f:
            items = json.load(f)
            
        for p in items:
            # Lấy quantity_sold an toàn
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
                quantity_sold, category_name, crawl_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                p.get("id"), p.get("sku"), p.get("name"), p.get("url_key"), 
                p.get("price", 0), p.get("original_price", 0), p.get("discount", 0), 
                p.get("discount_rate", 0), p.get("brand_name", ""), 
                p.get("rating_average", 0), p.get("review_count", 0), 
                p.get("thumbnail_url", ""), p.get("category_id"), sold_val, 
                p.get("category_name", ""), p.get("crawl_date", "")
            ))
        
        total_inserted += len(items)
        conn.commit()
        
    print(f"Successfully inserted {total_inserted} products into {db_path}!")
    
    # Tạo Index để query nhanh
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_category_id ON products(category_id)')
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
