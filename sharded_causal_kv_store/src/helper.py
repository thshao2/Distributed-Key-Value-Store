"""Useful Request and Asynchronous Helpers."""

from typing import Dict
from shared_data import SharedData  
import httpx
from fastapi.responses import JSONResponse

# Configure number of retries & timeout (in secs) here. 
RETRIES = 3
TIMEOUT = 2

class ReqHelper:
  @staticmethod
  def extract_msg_num_header(request) -> int | None:
    msg_num = request.headers.get("Msg-Num")
    if msg_num:
      return int(msg_num)
    return None

  @staticmethod
  def extract_node_id_header(request) -> int | None:
    node_id = request.headers.get("Node-Id")
    if node_id:
      return int(node_id)
    return None

  """Helper function to create the request headers.
  
  Usage: When returning the response from an endpoint, do:
    return <json response>, <status code>, create_res_headers(node_id=<node_id>)
  """
  @staticmethod
  def create_req_headers(node_id=SharedData.NODE_IDENTIFIER) -> Dict[str, int]:
    return {
      "Node-Id": str(node_id)
    }

  
class AsyncHelper:
  @staticmethod
  def config_retries(retries=RETRIES):
    """Used to create a transport object with the specified retries."""
    if not retries:
      return None
    return httpx.AsyncHTTPTransport(retries=retries)

  @staticmethod
  async def async_get(url, body={}, headers=None, timeout=TIMEOUT, retries=RETRIES):
    """
    Args: 
      url: destination url
      headers: JSON of the headers to be included
      timeout: timeout in seconds (set default at top of file here)
      retries number of retries (set default at top of file here)
    """
    async with httpx.AsyncClient(transport=AsyncHelper.config_retries(retries)) as client:
      response = await client.request("GET", url, json=body, headers=headers, timeout=timeout)
      return response
  
  @staticmethod
  async def async_post(url, body, headers=None, timeout=TIMEOUT, retries=RETRIES):
    async with httpx.AsyncClient(transport=AsyncHelper.config_retries(retries)) as client:
      print("body is", body)
      response = await client.post(url, json=body, headers=headers, timeout=timeout)
      return response

  @staticmethod
  async def async_put(url, body, headers=None, timeout=TIMEOUT, retries=RETRIES):
    async with httpx.AsyncClient(transport=AsyncHelper.config_retries(retries)) as client:
      response = await client.put(url, json=body, headers=headers, timeout=timeout)
      return response

  async def async_delete(url, body=None, headers=None, timeout=TIMEOUT, retries=RETRIES):
    async with httpx.AsyncClient(transport=AsyncHelper.config_retries(retries)) as client:
      response = await client.delete(url, headers=headers, timeout=timeout)
      return response

  @staticmethod
  def extract_res(response):
    """Returns a tuple of (body, status_code, headers)."""
    body = response.json()
    status_code = response.status_code
    headers = response.headers
    return (body, status_code, headers)
  
  @staticmethod
  def format_fast_api_res(httpx_res) -> JSONResponse:
    """Given an httpx response (returned from `async_get, async_put, etc),
       return a JSONResponse that can be used as a FastAPI Response.

       Use this for relaying the response from a request to the client.
    """
    body, status, headers = AsyncHelper.extract_res(httpx_res)
    return JSONResponse(content=body, status_code=status, headers=headers)
