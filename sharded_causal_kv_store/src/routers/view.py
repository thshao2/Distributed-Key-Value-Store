from fastapi import APIRouter, Request, Response, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from shared_data import SharedData
import util
from helper import ReqHelper, AsyncHelper
from packages.vector_clock import VectorClock
import util
import asyncio
import random

view_router = APIRouter()

@view_router.get('/view')
async def get_view():
    return JSONResponse(content=SharedData.current_view)

@view_router.put('/view')
async def update_view(request: Request, background_tasks: BackgroundTasks):
    try:
        # Parse the JSON body
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON format")

    # Check if "view" is in the request body
    if not data or "view" not in data:
        raise HTTPException(status_code=400, detail='Request body must have "view" field.')

    # Update the current view
    SharedData.current_view = data["view"]

    # update shard and hash info
    SharedData.current_shard = util.find_shard_by_node(SharedData.current_view, SharedData.NODE_IDENTIFIER)
    SharedData.shards = list(SharedData.current_view.keys())
    SharedData.hash_circle.update_shards(SharedData.shards)
    if SharedData.current_shard:
        SharedData.gossip_nodes = data["view"][SharedData.current_shard]
    random.shuffle(SharedData.gossip_nodes)

    # update view of clocks in causal data
    view_clock = VectorClock(util.extract_ids(SharedData.current_view))
    for dependencies in SharedData.causal_data.values():
        for clock in dependencies.values():
            clock.update_view(view_clock)

    # foward shard kvs and metadata to new nodes
    
    headers = ReqHelper.create_req_headers()
    # extract new location of keys
    key_locations = SharedData.hash_circle.redistribute_keys(list(SharedData.kvstore.keys()))
    copy_requests = []
    for shard in SharedData.shards:
        # compute payload info
        shard_kvs = {key: SharedData.kvstore[key] for key in key_locations[shard]}
        shard_metadata = {key: SharedData.causal_data[key] for key in key_locations[shard]}

        payload = {
            "kvstore": shard_kvs,
            "causal-metadata": util.server_metadata_to_dict(shard_metadata),
            "type": "view_change"
        }
        for shard_node in SharedData.current_view[shard]:
            address = shard_node['address']
            node_id = shard_node['id']
            # foward data if not yourself
            if str(node_id) != SharedData.NODE_IDENTIFIER:
                r = AsyncHelper.async_put(f"http://{address}/copy", payload, headers, timeout=1, retries=2)
                copy_requests.append(r)
    # wait for all copy requests to finish
    for request in copy_requests:
        await request
    # Check if the current view contains this node
    if not util.in_current_view():
        # Not in the new view => effectively do nothing
        return JSONResponse(content={"message": "Not in View"}, status_code=200)

    # update own kvs storage and metadata
    for shard, keys in key_locations.items():
        if shard != SharedData.current_shard:
            for key in keys:
                del SharedData.kvstore[key]
                del SharedData.causal_data[key]
    # SharedData.kvstore = {key: SharedData.kvstore[key] for key in key_locations[SharedData.current_shard]}
    # SharedData.causal_data = {key: SharedData.causal_data[key] for key in key_locations[SharedData.current_shard]}
    # Respond with the updated view
    return JSONResponse(content={"message": "View updated", "node_id": SharedData.NODE_IDENTIFIER, "shard": SharedData.current_shard, "key_map": key_locations}, status_code=200)

@view_router.put('/copy')
async def put_copy(request: Request, response: Response):
    """
    Internal endpoint for copying node data

    Expects JSON: {
      "kvstore": {<kvs key>: <kvs value>, ...}
      "causal-metadata": {<kvs key>: <VectorClock>, ...}
    }
    """
    # Get request json and throw error if nonexistent.
    try: 
        data = await request.json()
    except ValueError as e: # No json body.
        print("No json from request.")
        print(e)
        response.status_code = 400
        return {}

    server_kvstore: dict = data.get("kvstore")
    server_metadata: dict = data.get("causal-metadata")

    if server_kvstore is not None and server_metadata is not None:
        server_metadata = util.dict_to_server_metadata(server_metadata)
        for key, server_dependencies in server_metadata.items():
            # If the key doesn't belong in our shard, don't merge, and not doing view change
            if data.get("type") != "view_change" and not util.key_in_current_shard(key):
                continue
                
            # convert dicts in dependencies to vector clocks
            self_dependencies = SharedData.causal_data.get(key, {})
            util.update_metadata(self_dependencies, server_dependencies, SharedData.kvstore, server_kvstore, key)
            SharedData.causal_data[key] = self_dependencies
        return JSONResponse({"message": f"Replicated data for {SharedData.NODE_IDENTIFIER}"}, status_code=200)
    else:
        return JSONResponse({"error": "No kvstore or causal-metadata"}, status_code=400)

@view_router.get('/copy')
async def get_copy(request: Request, response: Response):
    """
    Internal endpoint for requesting node metadata and kvs for view changes

    Can also be used for testing if you feel like it
    """
    # return node_id, kvs, and causal metadata for a node

    return JSONResponse({
        "node_id": SharedData.NODE_IDENTIFIER, 
        "kvs": SharedData.kvstore, 
        "causal-metadata": util.server_metadata_to_dict(SharedData.causal_data)
    }, status_code=200)
    