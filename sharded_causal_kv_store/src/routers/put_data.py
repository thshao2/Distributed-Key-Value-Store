from fastapi import APIRouter, Request, Response, BackgroundTasks
from fastapi.responses import JSONResponse
from shared_data import SharedData
from packages.vector_clock import VectorClock
from packages.broadcast import broadcast_info

from helper import AsyncHelper

import util
import asyncio

put_data_router = APIRouter()

@put_data_router.put('/data/{key}')
async def put_data(key: str, response: Response, request: Request, background_tasks: BackgroundTasks):
    """
    1. Parse request body, which includes a 'value' and possibly 'causal-metadata'.
    2. Check if this node is in the current view (else 503).
    3. Extract client's causal-metadata if present.
    4. Merge client metadata that is less updated than this node's causal metadata.
    5. Increment local vector clock.
    6. Store the key -> value in kvstore.
    7. Respond with updated causal-metadata (which may or may not have been updated).
    8. Replicate/gossip to other nodes.
    """

    # Get request json and throw error if nonexistent.
    try: 
        data = await request.json()
    except ValueError as e: # No json body.
        print(e)
        response.status_code = 400
        return {}

    # Check input.
    if "value" not in data or not isinstance(data["value"], str):
        return JSONResponse(content={"error": 'Missing or invalid JSON body. Expected { "value": "string" }'}, status_code=400)
    
    # Check if node is in current view, else 503.
    if not util.in_current_view():
        return JSONResponse({"error": "Node not in view"}, status_code=503)
    
    if (not util.key_in_current_shard(key)):
        forwardShard = SharedData.hash_circle.get_shard_for_key(key)
        print("Forward Shard:", forwardShard)
        potentialNodes = util.get_nodes_by_shard(SharedData.current_view, forwardShard)
        print("Potential Nodes: ", potentialNodes)
        
        tasks = []
        for node in potentialNodes:
            address = node["address"]
            url = f"http://{address}/data/{key}"

            # forward the same PUT request body
            tasks.append(AsyncHelper.async_put(url, data, timeout=3))
        
        # Run all proxy tasks concurrently, continue until response is received
        while True:
            print("Proxying concurrently, in the while loop...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Check if any proxy returned a successful response
            for result in results:
                if not isinstance(result, Exception) and result.status_code < 400:
                    return AsyncHelper.format_fast_api_res(result)
            
            await asyncio.sleep(1)

    # 1. Extract client's causal-metadata
    client_metadata = util.dict_to_causal_data(data.get("causal-metadata", dict()))
    value = data['value']

    # 2. add client dependencies to server_metadata for this key
    server_key_metadata = SharedData.causal_data.get(key, {key: VectorClock(util.extract_ids(SharedData.current_view))})
    for dep_key, dep_clock in client_metadata.items():
        old_server_clock = server_key_metadata.get(dep_key, VectorClock(util.extract_ids(SharedData.current_view)))
        # new clock is pairwise max of client and server's clock
        server_key_metadata[dep_key] = old_server_clock.pairwise_max(dep_clock)

    # 3. Increment 1 for node's position in vector clock data for the key
    server_key_metadata[key][SharedData.NODE_IDENTIFIER] += 1 

    # assign updated metadata to server if not there already
    SharedData.causal_data[key] = server_key_metadata

    # 4. put the value into the kvs
    SharedData.kvstore[key] = value # 2. Store PUT request value in kvstore

    updatedMetadata = util.causal_data_to_dict(server_key_metadata)
    
    # 5. Replicate (Broadcast) to other nodes, retry every some seconds (or await for gossip protocol)
    background_tasks.add_task(broadcast_info, background_tasks, "PUT", updatedMetadata, key, value)

    # send back the updatedMetadata server metadata 
    return JSONResponse({
        "message": "Key updated successfully." if key in SharedData.kvstore else "Key created successfully.",
        "causal-metadata": updatedMetadata
    }, status_code=200)
