import os
import sqlite3
import math
from fastapi import FastAPI, Query
from typing import Optional

app = FastAPI(title="Mock Tiki Backend API")

def get_db_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(current_dir, "..", "..", "data"))
    db_path = os.path.join(data_dir, "tiki_backend.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/api/personalish/v1/blocks/listings")
def get_products(
    category: int = Query(..., description="Category ID"),
    page: int = Query(1, description="Page number"),
    limit: int = Query(40, description="Items per page")
):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Đếm tổng số sản phẩm
    cursor.execute("SELECT COUNT(1) FROM products WHERE category_id = ?", (category,))
    total_items = cursor.fetchone()[0]

    # Lấy dữ liệu phân trang
    offset = (page - 1) * limit
    cursor.execute("""
        SELECT * FROM products
        WHERE category_id = ?
        LIMIT ? OFFSET ?
    """, (category, limit, offset))

    rows = cursor.fetchall()
    conn.close()

    # Convert sqlite3.Row to dict
    items = [dict(row) for row in rows]

    total_pages = math.ceil(total_items / limit) if limit > 0 else 1

    return {
        "data": items,
        "paging": {
            "current_page": page,
            "last_page": total_pages,
            "per_page": limit,
            "total": total_items
        }
    }

if __name__ == "__main__":
    import uvicorn
    print("Khởi động Mock Tiki Service tại http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
