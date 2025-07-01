from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from shared_data import SharedData
from packages.vector_clock import VectorClock

import util
import asyncio

from helper import AsyncHelper

get_data_router = APIRouter()

@get_data_router.get('/data')
async def get_all_data(request: Request, response: Response):
    """
    1. Extract client_causal_meta from request.
    2. Possibly hang if the node hasn't caught up to the client's metadata.
    3. Return all key, value pairs in this node otherwise and the update client metadata if neccessary.
    """
    # Node is not in view, return 503.
    if not util.in_current_view():
        return JSONResponse({"error": f"Node {SharedData.NODE_IDENTIFIER} is not in view"}, status_code=503)

    # Get request json and throw error if nonexistent.
    try: 
        data = await request.json()
    except ValueError as e: # No json body.
        data = {}

    causal_metadata = util.dict_to_causal_data(data.get("causal-metadata", dict()))

    for key, vc in causal_metadata.items():
        # If the node/shard isn't responsible for this key, skip
        if not util.key_in_current_shard(key):
            continue
        
        client_key_vc: VectorClock = vc # Client Vector Clock for Key
        local_key_vc: VectorClock = SharedData.causal_data.get(key, {}).get(key, None) # What is the local clock for this key?

        # Local VC for key is either concurrent (breaking the tie), more updated, or exactly equal to the client. This key is good.
        if (local_key_vc and not (local_key_vc < client_key_vc) and (client_key_vc == local_key_vc or local_key_vc.concurrent_break_ties(client_key_vc) == local_key_vc)):
            continue
        else:
            await wait_until_caught_up(key, vc) # Hang, this key is not updated to client metadata

    # get updated metadata
    json_updated_metadata = util.assemble_get_all_metadata_dict(SharedData.causal_data, causal_metadata)
    
    return JSONResponse({
        "items": SharedData.kvstore,
        "causal-metadata": json_updated_metadata,
    }, status_code=200)
    

@get_data_router.get('/data/{key}')
async def get_data(key: str, response: Response, request: Request):
    """
    1. Extract client_causal_meta from request.
    2. Hang if the node hasn't caught up to the client's metadata.
    3. Return the local value for 'key' and the update client metadata if neccessary.
    """

    # If node is not in view, return 503.
    if not util.in_current_view():
        return JSONResponse({"error": f"Node {SharedData.NODE_IDENTIFIER} is not in view"}, status_code=503)

    # Get request json and throw error if nonexistent.
    try: 
        data = await request.json()
    except ValueError as e: # No json body.
        print(f"error in get_data on {key}: {e}")
        data = {}
        # response.status_code = 400
        # return {}
    
    if (not util.key_in_current_shard(key)):
        forwardShard = SharedData.hash_circle.get_shard_for_key(key)
        potentialNodes = util.get_nodes_by_shard(SharedData.current_view, forwardShard)
        print("Get potential nodes:", potentialNodes)
        
        tasks = []
        for node in potentialNodes:
            address = node["address"]
            url = f"http://{address}/data/{key}"

            # forward the same GET request body
            tasks.append(AsyncHelper.async_get(url, body=data, timeout=3))
        
        # Run all proxy tasks concurrently, continue until response is received
        while True:
            print("Proxying for get, waiting for reply...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Check if any proxy returned a successful response
            for result in results:
                if not isinstance(result, Exception) and result.status_code < 400:
                    return AsyncHelper.format_fast_api_res(result)
            
            await asyncio.sleep(1)

    # extract metadata from client and server
    client_metadata: dict[str, VectorClock] = util.dict_to_causal_data(data.get("causal-metadata", dict()))
    server_key_metadata: dict[str, VectorClock] = SharedData.causal_data.get(key, dict())
    
    # get clocks to determine causal consistency
    # if no clock, just make an empty one to compare with
    client_dep_clock: VectorClock = client_metadata.get(key, VectorClock(util.extract_ids(SharedData.current_view)))
    server_dep_clock: VectorClock = server_key_metadata.get(key, VectorClock(util.extract_ids(SharedData.current_view)))


    # check if current key value is up to date with client_clock
    if client_dep_clock > server_dep_clock or (server_dep_clock.isConcurrent(client_dep_clock) and 
        server_dep_clock.concurrent_break_ties(client_dep_clock) == client_dep_clock):
        
        # Hang until gossip protocol takes effect, broadcast/request for key (polling approach)
        await wait_until_caught_up(key, client_dep_clock) # Hang

        # Get new dependencies for server key
        server_key_metadata: dict[str, VectorClock] = SharedData.causal_data.get(key, dict())

    # add each dependency for this key to the client's metadata
    for dep_key, dep_clock in server_key_metadata.items():
        old_client_clock: VectorClock = client_metadata.get(dep_key, VectorClock(util.extract_ids(SharedData.current_view)))
        # do not update clock if concurrent read and we win the tiebreaker 
        if dep_key == key and (server_dep_clock.isConcurrent(old_client_clock) and 
            server_dep_clock.concurrent_break_ties(old_client_clock) == old_client_clock):
            continue
        # new clock is pairwise max of client and server's clock
        client_metadata[dep_key] = old_client_clock.pairwise_max(dep_clock)
    
    #convert metadata to json
    updated_metadata = util.causal_data_to_dict(client_metadata)
    
    # return value if it exists
    if key in SharedData.kvstore:
        return JSONResponse({
            "value": SharedData.kvstore[key],
            "causal-metadata": updated_metadata,
        }, status_code=200)
    
    # otherwise 404 error
    else:
        return JSONResponse({
            "message": "Key Not Found",
            "causal-metadata": util.causal_data_to_dict(client_metadata),
        }, status_code=404)
        

async def wait_until_caught_up(key: str, client_vc: VectorClock):
    # If the node/shard isn't responsible for this key, no need to wait
    if not util.key_in_current_shard(key):
        return

    # Poll every 1.5s
    while True:
        local_vc: VectorClock = SharedData.causal_data.get(key, dict()).get(key)
        print(f"Wait for {key}. Client VC is {client_vc}, local is {local_vc}")
        if local_vc is not None and (local_vc >= client_vc or (local_vc.isConcurrent(client_vc) and local_vc.concurrent_break_ties(client_vc) == local_vc)):
            break
        # Yield control to the event loop and try again
        await asyncio.sleep(1.5)

