"""Useful Request and Response Helpers."""

from typing import Dict
from shared_data import SharedData  

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
    return <json response>, <status code>, create_res_headers(msg_num, node_id=<node_id>)
  """
  @staticmethod
  def create_req_headers(msg_num: int, node_id=SharedData.NODE_IDENTIFIER) -> Dict[str, int]:
    return {
      "Node-Id": str(node_id),
      "Msg-Num": str(msg_num)
    }