# filepath: /workspace/libs/tiki_product.py
import os
import sys
import time
import json

current_dir = os.path.dirname(os.path.abspath(__file__))
workspace_dir = os.path.abspath(os.path.join(current_dir, '..'))
if workspace_dir not in sys.path:
    sys.path.append(workspace_dir)

from libs.http_client import HttpClient
from libs.utils import setup_logger

logger = setup_logger(__name__)

def fetch_products_by_category(category_id, url_key, max_pages=None):
    """
    Quét toàn bộ sản phẩm của 1 danh mục lá qua API listings.
    Tự động dừng khi hết trang hoặc mảng data trả về rỗng.
    """
    client = HttpClient()
    api_url = "https://tiki.vn/api/personalish/v1/blocks/listings"
    
    all_products = []
    seen_ids = set() # Dùng set để loại bỏ sản phẩm trùng lặp
    
    current_page = 1
    last_page = 1 
    
    while True: 
        if max_pages and current_page > max_pages:
            logger.info(f"Đã đạt giới hạn test max_pages={max_pages}. Dừng crawl.")
            break
            
        params = {
            "limit": 40,
            "include": "advertisement",
            "aggregations": 2,
            "version": "home-persionalized",
            "category": category_id,
            "urlKey": url_key,
            "page": current_page
        }
        
        logger.info(f"Danh mục {category_id} ({url_key}) - Đang kéo trang {current_page}/{last_page}")
        
        data = client.get(api_url, params=params)
        
        if not data:
            logger.error(f"Lỗi gọi API tại danh mục {category_id}, trang {current_page}. Dừng crawl danh mục này.")
            break
            
        items = data.get('data', [])
        
        # Điều kiện dừng: Mảng data rỗng 
        if not items or len(items) == 0:
            logger.info(f"Đã hết sản phẩm ở trang {current_page}. Hoàn thành danh mục {category_id}.")
            break
            
        for item in items:
            product_id = item.get("id")
            
            # --- KIỂM TRA TRÙNG LẶP ---
            if product_id in seen_ids:
                continue
            seen_ids.add(product_id)
            
            qty_sold = item.get("quantity_sold")
            sold_value = 0
            if isinstance(qty_sold, dict):
                sold_value = qty_sold.get("value", 0)
            elif isinstance(qty_sold, int):
                sold_value = qty_sold

            original_price = item.get("original_price") or item.get("list_price", 0)

            product = {
                "id": product_id,
                "sku": item.get("sku"),
                "name": item.get("name"),
                "url_key": item.get("url_key"),
                "price": item.get("price", 0),
                "original_price": original_price, 
                "discount": item.get("discount", 0),
                "discount_rate": item.get("discount_rate", 0),
                "brand_name": item.get("brand_name", "No Brand"),
                "rating_average": item.get("rating_average", 0),
                "review_count": item.get("review_count", 0),
                "thumbnail_url": item.get("thumbnail_url", ""),
                "category_id": category_id,
                "quantity_sold": sold_value
            }
            all_products.append(product)
        
        # Cập nhật last_page từ metadata của API để biết tiến độ
        paging = data.get('paging', {})
        last_page = paging.get('last_page', 1)
        
        # Điều kiện dừng an toàn phụ: Nếu current_page vượt quá last_page API báo
        if current_page >= last_page:
            logger.info(f"Đã chạm trang cuối ({last_page}). Hoàn thành danh mục {category_id}.")
            break
            
        current_page += 1
        time.sleep(2)
        
    return all_products

"""
if __name__ == "__main__":
    print("--- BẮT ĐẦU TEST CRAWL FULL TRANG ---")
    
    # Test bằng đúng category bạn vừa gửi: Mặt nạ đất sét (id: 11709)
    test_category_id = 11711
    test_url_key = "mat-na-ngu"
    
    # Để trống max_pages để script tự chạy đến khi hết trang 3 và tự dừng
    products = fetch_products_by_category(test_category_id, test_url_key)
    
    print(f"\n ĐÃ XONG! Lọc bỏ trùng lặp quảng cáo, tổng số sản phẩm lấy được: {len(products)}")
    
    if products:
        print("\n--- Sản phẩm đầu tiên ---")
        print(json.dumps(products[0], ensure_ascii=False, indent=4))
        print("\n--- Sản phẩm cuối cùng ---")
        print(json.dumps(products[-1], ensure_ascii=False, indent=4))

"""