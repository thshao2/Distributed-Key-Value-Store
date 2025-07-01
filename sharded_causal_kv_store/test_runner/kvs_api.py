import requests
from typing import Dict, Any, List

import aiohttp

"""
Request Timeout status code.
Technically not proper as the server should return the 408 in tradititional usage,
but it is a good enough indicator for our purposes
"""
REQUEST_TIMEOUT_STATUS_CODE = 408
DEFAULT_TIMEOUT = 10


def create_json(metadata, value=None):
    result = {"causal-metadata": {}}
    if metadata is not None:
        result["causal-metadata"] = metadata
    if value is not None:
        result["value"] = value
    return result


# client for kvs api


class KVSClient:
    def __init__(self, base_url: str):
        # set base url without trailing slash
        self.base_url = base_url if not base_url.endswith("/") else base_url[:-1]

    def ping(self, timeout: float = DEFAULT_TIMEOUT) -> requests.Response:
        if timeout is not None:
            try:
                return requests.get(f"{self.base_url}/ping", timeout=timeout)
            except requests.exceptions.Timeout:
                r = requests.Response()
                r.status_code = REQUEST_TIMEOUT_STATUS_CODE
                return r
        else:
            return requests.get(f"{self.base_url}/ping")

    def get(
        self, key: str, metadata: str, timeout: float = DEFAULT_TIMEOUT
    ) -> requests.Response:
        if not key:
            raise ValueError("key cannot be empty")

        if timeout is not None:
            try:
                return requests.get(
                    f"{self.base_url}/data/{key}",
                    json=create_json(metadata),
                    timeout=timeout,
                )
            except requests.exceptions.Timeout:
                r = requests.Response()
                r.status_code = REQUEST_TIMEOUT_STATUS_CODE
                return r
        else:
            return requests.get(
                f"{self.base_url}/data/{key}", json=create_json(metadata)
            )

    def put(
        self, key: str, value: str, metadata: str, timeout: float = DEFAULT_TIMEOUT
    ) -> requests.Response:
        if not key:
            raise ValueError("key cannot be empty")

        if timeout is not None:
            try:
                return requests.put(
                    f"{self.base_url}/data/{key}",
                    json=create_json(metadata, value),
                    timeout=timeout,
                )
            except requests.exceptions.Timeout:
                r = requests.Response()
                r.status_code = REQUEST_TIMEOUT_STATUS_CODE
                return r
        else:
            return requests.put(
                f"{self.base_url}/data/{key}", json=create_json(metadata, value)
            )

    def delete(self, key: str, timeout: float = DEFAULT_TIMEOUT) -> requests.Response:
        if not key:
            raise ValueError("key cannot be empty")

        if timeout is not None:
            try:
                return requests.delete(f"{self.base_url}/data/{key}", timeout=timeout)
            except requests.exceptions.Timeout:
                r = requests.Response()
                r.status_code = REQUEST_TIMEOUT_STATUS_CODE
                return r
        else:
            return requests.delete(f"{self.base_url}/data/{key}")

    def get_all(
        self, metadata: str, timeout: float = DEFAULT_TIMEOUT
    ) -> requests.Response:
        if timeout is not None:
            try:
                return requests.get(
                    f"{self.base_url}/data", json=create_json(metadata), timeout=timeout
                )
            except requests.exceptions.Timeout:
                r = requests.Response()
                r.status_code = REQUEST_TIMEOUT_STATUS_CODE
                return r
        else:
            return requests.get(f"{self.base_url}/data", json=create_json(metadata))

    def clear(self, timeout: float = DEFAULT_TIMEOUT) -> None:
        response = self.get_all(timeout=timeout)
        if response.status_code != 200:
            raise RuntimeError(f"failed to get keys: {response.status_code}")

        for key in response.json().keys():
            delete_response = self.delete(key, timeout=timeout)
            if delete_response.status_code != 200:
                raise RuntimeError(
                    f"failed to delete key {key}: {delete_response.status_code}"
                )

    def send_view(
        self, view: dict[str, List[Dict[str, Any]]], timeout: float = DEFAULT_TIMEOUT
    ) -> requests.Response:
        if not isinstance(view, dict):
            raise ValueError("view must be a dict")

        self.last_view = view
        request_body = {"view": view}
        return requests.put(f"{self.base_url}/view", json=request_body, timeout=timeout)

    async def async_send_view(
        self, view: dict[str, List[Dict[str, Any]]], timeout: float = DEFAULT_TIMEOUT
    ) -> aiohttp.ClientResponse:
        if not isinstance(view, dict):
            raise ValueError("view must be a dict")

        self.last_view = view
        request_body = {"view": view}

        async with aiohttp.ClientSession() as session:
            async with session.put(
                f"{self.base_url}/view", json=request_body, timeout=timeout
            ) as response:
                return response
            if response.status != 200:
                raise RuntimeError(f"failed to send view: {response.status}")
            return response

    def resend_last_view_with_ips_from_new_view(
        self,
        current_view: dict[str, List[Dict[str, Any]]],
        timeout: float = DEFAULT_TIMEOUT,
    ) -> requests.Response:
        if not isinstance(current_view, dict):
            raise ValueError("view must be a dict")
        if not hasattr(self, "last_view"):
            return
            raise LookupError("Must have sent at least one view before calling resend.")
        flattened_current_view = {}
        for shard_key in current_view:
            for node in current_view[shard_key]:
                flattened_current_view[node["id"]] = node["address"]

        for shard_key in self.last_view:
            for node in self.last_view[shard_key]:
                node["address"] = flattened_current_view[node["id"]]

        request_body = {"view": self.last_view}
        print(f"Sending new view: {self.last_view}")
        return requests.put(f"{self.base_url}/view", json=request_body, timeout=timeout)
