import asyncio
from shared_data import SharedData
from helper import TIMEOUT, RETRIES, AsyncHelper, ReqHelper
import util
from packages.vector_clock import VectorClock

"""Helpers for broadcasting/relaying PUT/DELETE requests to other nodes."""

async def broadcast_info(background_tasks, operation, causal_metadata, key, value=None):
    """Called by PUT / DELETE endpoints - broadcast info to all other nodes.

    It starts one background task (separate thread) per node that it needs to
        communicate to.

    Args:
        background_tasks: fastapi's BackgroundTasks
        operation: "PUT" or "DELETE"
        causal_metadata: {<kvs key>: <VectorClock>, ...} for the specific key
        key: key to perform the operation on
        value: only for PUT
    """
    print("Starting broadcast for key", key, "val", value)
    if not causal_metadata or causal_metadata == {}:
        causal_metadata_json = {}

    # If values in causal_metadata are type VectorClock, convert to dict.
    else:
        causal_metadata_json = util.causal_data_to_dict(causal_metadata)

    # Assemble payload to be attached to internal update endpoint.
    payload = {
        "operation": operation,
        "key": key,
        "causal-metadata": causal_metadata_json
    }
    if operation == "PUT":
        payload["value"] = value
    
    # Iterate for all nodes in the same shard execept itself.
    for node in SharedData.gossip_nodes:
        # Get node address / id.
        node_addr = node["address"]
        node_id = node["id"]
        if int(node_id) == int(SharedData.NODE_IDENTIFIER):
            continue

        headers = ReqHelper.create_req_headers()

        # Start a background task to send an update request to node.
        background_tasks.add_task(unicast_info, f"http://{node_addr}", payload, headers)
    print("Finished broadcast for key", key, "val", value)
    return True

async def unicast_info(node_addr, payload, headers):
    """Unicast to the node for a PUT/DELETE using best effort reliable delivery.

    It will timeout after an interval, and a finite number of times.
        - timeout and retries are configured at the top of `src/helper.py`
    
    Args:
        node_addr: address to send the request to.
        payload: JSON consisting of {
                "operation": "PUT" or "DELETE",
                "key": "<key>",
                "value": "<val>" # only for PUT,
                "causal-metadata": {<kvs key>: <VectorClock>, ...}
            }
        headers: JSON consisting of {
                "Node-Id": <current node's ID>
            }
    """
    print(f"Starting unicast to {node_addr}")
    r = await AsyncHelper.async_post(f"{node_addr}/update", payload, headers=headers)
    print(f"Unicast to {node_addr} done. {AsyncHelper.extract_res(r)}")

