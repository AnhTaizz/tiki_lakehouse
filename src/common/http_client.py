import os
import requests
import random
import time
from common.utils import setup_logger

logger = setup_logger(__name__)

# URL của Mock Service (chạy trên máy Host, gọi từ trong Docker dùng host.docker.internal)
# Nếu chạy ở máy thật không qua Docker, đổi thành localhost
MOCK_API_BASE_URL = os.getenv("MOCK_API_BASE_URL", "http://host.docker.internal:8000")

class HttpClient:
    def __init__(self):
        fingerprints = [
            "chrome116", "chrome115", "chrome114",
            "firefox115", "firefox114", "firefox135",
            "safari16", "safari15"
        ]
        fp = random.choice(fingerprints)

        logger.info("Initialized HttpClient pointed to Mock Service: %s (Fingerprint: %s)", MOCK_API_BASE_URL, fp)

    def get(self, url, params=None, headers=None):
        """
        Thay vì gọi lên Tiki thật, chúng ta "bẻ lái" sang gọi Mock Service (FastAPI) cục bộ.
        Điều này giúp mô phỏng 100% việc kết nối mạng HTTP thực tế.
        """
        # Bẻ lái URL: Lấy endpoint sau domain và ráp vào MOCK_API_BASE_URL
        # Ví dụ: url = https://tiki.vn/api/personalish/v1/blocks/listings
        # Ta biến thành http://host.docker.internal:8000/api/personalish/v1/blocks/listings
        endpoint = "/api/personalish/v1/blocks/listings"
        mock_url = f"{MOCK_API_BASE_URL}{endpoint}"

        time.sleep(random.uniform(0.01, 0.05))

        try:
            response = requests.get(mock_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            total_items = len(data.get("data", []))
            if total_items == 0:
                logger.info("MOCK_API: Returning 0 items for category %s (Page %s/0)", params.get("category"), params.get("page", 1))

            return data

        except requests.exceptions.RequestException as e:
            logger.error("Failed to call Mock Service at %s: %s", mock_url, e)
            return {"data": [], "paging": {"last_page": 0}}
