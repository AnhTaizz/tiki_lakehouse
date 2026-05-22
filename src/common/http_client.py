# filepath: /workspace/libs/http_client.py
import requests
import time
from libs.utils import setup_logger

logger = setup_logger(__name__)

class HttpClient:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*"
        }

    """Hàm GET an toàn, tự động thử lại nếu lỗi"""
    def get(self, url, params=None, max_retries=3, delay=3):
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=10)
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning(f"Lỗi {response.status_code} khi gọi {url}. Thử lại {attempt + 1}/{max_retries}")
            except Exception as e:
                logger.error(f"Lỗi kết nối: {e}")
            
            time.sleep(delay)
        return None