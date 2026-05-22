import time
from curl_cffi import requests
from common.utils import setup_logger

logger = setup_logger(__name__)

class HttpClient:
    def __init__(self):
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
        }

    def get(self, url, params=None, max_retries=3, delay=3):
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url, headers=self.headers, params=params, timeout=10, impersonate="chrome120"
                )
                if response.status_code == 200:
                    return response.json()

                logger.warning(
                    "HTTP %s when calling %s. Retry %s/%s",
                    response.status_code,
                    url,
                    attempt + 1,
                    max_retries,
                )
            except Exception as exc:
                logger.error("Connection error: %s", exc)

            time.sleep(delay)

        return None

