import requests
from typing import Dict, Any, Optional
from urllib.parse import urljoin


# client for kvs api
class KVSClient:
    def __init__(self, base_url: str):
        # set base url without trailing slash
        self.base_url = base_url if not base_url.endswith("/") else base_url[:-1]

    def ping(self) -> requests.Response:
        return requests.get(f"{self.base_url}/ping")

    def get(self, key: str) -> requests.Response:
        if not key:
            raise ValueError("key cannot be empty")
        return requests.get(f"{self.base_url}/data/{key}")

    def put(self, key: str, value: str) -> requests.Response:
        if not key:
            raise ValueError("key cannot be empty")
        return requests.put(f"{self.base_url}/data/{key}", json={"value": value})

    def delete(self, key: str) -> requests.Response:
        if not key:
            raise ValueError("key cannot be empty")
        return requests.delete(f"{self.base_url}/data/{key}")

    def get_all(self) -> requests.Response:
        return requests.get(f"{self.base_url}/data")

    def clear(self) -> None:
        response = self.get_all()
        if response.status_code != 200:
            raise RuntimeError(f"failed to get keys: {response.status_code}")

        for key in response.json().keys():
            delete_response = self.delete(key)
            if delete_response.status_code != 200:
                raise RuntimeError(
                    f"failed to delete key {key}: {delete_response.status_code}"
                )

    def send_view(self, view: list[Dict[str, Any]]) -> requests.Response:
        if not isinstance(view, list):
            raise ValueError("view must be a list")

        request_body = {"view": view}
        return requests.put(f"{self.base_url}/view", json=request_body)
