#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests

class TikiAPIClient:
    """ Class chuyên dụng để gọi API Tiki lấy danh sách sản phẩm """
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*'
        }
        self.base_url = "https://tiki.vn/api/personalish/v1/blocks/listings"

    def fetch_products(self, page, category_id='1582', url_key='cham-soc-da-mat'):
        params = {
            'limit': 40,
            'include': 'advertisement',
            'aggregations': 2,
            'version': 'home-persionalized',
            'trackity_id': 'd15ceb6f-99c2-f45a-7a55-6a84842765b5',
            'category': category_id,
            'page': page,
            'urlKey': url_key
        }

        try:
            response = requests.get(self.base_url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                data = response.json().get('data', [])
                results = []

                for item in data:
                    qty_sold = item.get('quantity_sold', {})
                    sold_value = qty_sold.get('value', 0) if isinstance(qty_sold, dict) else 0
                    
                    # Bóc mảng impression_info
                    impressions = item.get('impression_info', [])
                    metadata = impressions[0].get('metadata', {}) if impressions else {}
                    
                    results.append({
                        'id': item.get('id'),
                        'sku': item.get('sku', ''),
                        'name': str(item.get('name', '')).replace('\n', ' ').replace(',', ' -'),
                        'brand_name': item.get('brand_name', 'Unknown'),
                        
                        # Nhóm Fact
                        'price': float(item.get('price', 0)),
                        'original_price': float(item.get('original_price', 0)),
                        'discount_rate': int(item.get('discount_rate', 0)),
                        'rating_average': float(item.get('rating_average', 0)),
                        'review_count': int(item.get('review_count', 0)),
                        'quantity_sold': int(sold_value),
                        
                        # Nhóm UI/UX cho Superset
                        'thumbnail_url': item.get('thumbnail_url', ''),
                        
                        # Nhóm Flag từ Metadata
                        'is_ad': int(metadata.get('is_ad', 0)),
                        'is_tikinow': int(metadata.get('is_tikinow', 0)),
                        'tiki_verified': int(metadata.get('tiki_verified', 0))
                    })
                return results
            else:
                print(f"Lỗi API (Code {response.status_code}) ở trang {page}")
                return []
                
        except Exception as e:
            print(f"Lỗi gọi API trang {page}: {e}")
            return []


# In[ ]:




