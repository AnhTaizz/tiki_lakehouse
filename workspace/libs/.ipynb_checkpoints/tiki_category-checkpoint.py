# filepath: /workspace/libs/tiki_category.py
import json
import os
import sys

# =========================================================
# FIX LỖI IMPORT: Giúp Python nhận diện được thư mục 'libs' 
# =========================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
workspace_dir = os.path.abspath(os.path.join(current_dir, '..'))
if workspace_dir not in sys.path:
    sys.path.append(workspace_dir)

from libs.http_client import HttpClient
from libs.utils import setup_logger

# =========================================================
# FIX LỖI THIẾU LOGGER
# =========================================================
logger = setup_logger(__name__)

"""Đọc file json danh mục"""
def load_categories_from_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    return raw_data.get('data', [])

"""Đọc cấu trúc json từ API"""
def load_categories_from_api(category_id=None):
    # =========================================================
    # FIX LỖI THIẾU CLIENT: Phải khởi tạo HttpClient thì mới gọi get() được
    # =========================================================
    client = HttpClient()
    
    base_url = "https://tiki.vn/api/v2/categories"
    params = {}
    params['include'] = 'children'
    if category_id:
        params['parent_id'] = category_id
        
    logger.info(f"Đang tải cấu trúc danh mục online từ Tiki (ID: {category_id if category_id else 'Gốc'})...")
    response_data = client.get(base_url, params=params)
    
    if response_data:
        return response_data.get('data', [])             # Bọc trong "data" trong cấu trúc json
    
    logger.error("Không thể lấy danh mục từ API.")    
    return []

"""Đệ quy danh sách tất cả các node lá"""
def get_leaf_categories(categories):
    leaf_nodes = []
    for cat in categories:
        if cat.get('is_leaf') == True or not cat.get('children'):
            leaf_nodes.append({
                'id': cat.get('id'),
                'name': cat.get('name'),
                'url_key': cat.get('url_key')
            })
        else:
            leaf_nodes.extend(get_leaf_categories(cat['children']))
    return leaf_nodes

if __name__ == "__main__":
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_filepath = os.path.join(current_dir, '..', 'data', 'lam_dep_suc_khoe_category.json')
    
    print(f"Đang tìm file tại: {test_filepath}")
    """
    # Test hàm load API trực tiếp
    print("\n--- TEST CALL API ONLINE ---")
    categories_tree = load_categories_from_api(1520)
    
    if categories_tree:
        print(f"Đọc API thành công! Tìm thấy {len(categories_tree)} danh mục gốc.")
        
        # 2. Test hàm đệ quy tìm node lá
        leafs = get_leaf_categories(categories_tree)
        print(f"Quét thành công! Tìm thấy tổng cộng {len(leafs)} danh mục lá (cấp cuối cùng).")
        
        # 3. In thử kết quả
        print("\n--- XEM THỬ 80 DANH MỤC LÁ ĐẦU TIÊN ---")
        print(json.dumps(leafs[:80], ensure_ascii=False, indent=4))
    else:
        print("Lỗi: Không kéo được dữ liệu từ API!")