# src/ingestion/client.py
"""PubgApiClient – handles authentication, request throttling, pagination, and retries.
Replace the stub with real implementation when ready.
"""

import os
import requests
from typing import Any, Dict

class PubgApiClient:
    def __init__(self):
        self.api_key = os.getenv("PUBG_API_KEY")
        self.base_url = "https://api.pubg.com"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/vnd.api+json",
        }

    def get(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
