"""
Helpers for broadcasting updates to other nodes. Used for PUT.
"""
from fastapi import APIRouter, Request, Response, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from shared_data import SharedData
from helper import ReqHelper, AsyncHelper

from packages.vector_clock import VectorClock
import packages.broadcast as broadcast

import requests
import util

update_data_router = APIRouter()

test_num = 0
@update_data_router.get('/test_broadcast')
async def test_broadcast(request: Request, response: Response, background_tasks: BackgroundTasks):
    """Test endpoint for checking broadcast. (DO NOT USE THIS ACTUALLY)
    
    - It'll do an update to KVS with key = "test_broadcast" and value of int(test_num).
    - Check the logs of the containers to ensure broadcast was successful.
    """
    global test_num
    # Get request json.
    try: 
        data = await request.json()
    except ValueError as e: # No json body.
        data = {}

    # Get causal_metadata and deserialize it from json. 
    causal_metadata: dict[str, VectorClock] = data.get("causal-metadata", {})
    causal_metadata = util.dict_to_causal_data(causal_metadata)

    # Initialize VC if key originally didn't exist in KVS.
    if "test_broadcast" not in causal_metadata:
        causal_metadata["test_broadcast"] = VectorClock(util.extract_ids(SharedData.current_view))
    
    #Increment Vector Clock.
    causal_metadata["test_broadcast"][str(SharedData.NODE_IDENTIFIER)] += 1
    SharedData.causal_data = causal_metadata.copy()

    # Broadcast to all other nodes
    await broadcast.broadcast_info(background_tasks, "PUT", causal_metadata, "test_broadcast", test_num)
    SharedData.kvstore["test_broadcast"] = test_num
    test_num += 1
    
    return JSONResponse({"kvs": SharedData.kvstore, "causal-metadata": util.causal_data_to_dict(causal_metadata)})

@update_data_router.post('/update')
async def update(request: Request, response: Response):
    """
    Internal endpoint for relaying PUT requests.

    Called by `unicast_info` in src/packages/broadcast.py.

    Expects JSON: {
      "key": "<key>",
      "value": "<val>" # only for PUT,
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

    # Extract headers & json body.
    sender_node_id = ReqHelper.extract_node_id_header(request)
    key = data["key"]
    val = data["value"]
    json_causal_metadata = data["causal-metadata"]

    causal_metadata = util.dict_to_causal_data(json_causal_metadata)
    
    # Get local and msg's VC for the specified key (initialize one if nonexistent).
    local_key_vc = SharedData.causal_data.get(key, {}).get(key, VectorClock(util.extract_ids(SharedData.current_view)))

    print("local_key_vc:", local_key_vc, type(local_key_vc))
    msg_key_vc: VectorClock = causal_metadata.get(key, VectorClock(local_key_vc.clock.keys()))
    print("msg_key_vc:", msg_key_vc, type(msg_key_vc))

    # Case 1: msg VC < local VC, don't deliver (local is more updated)
    if msg_key_vc < local_key_vc:
        return JSONResponse({
            "message": f"Update did not occur - local VC {local_key_vc} more ahead than message's VC {msg_key_vc}"
        })

    # Case 2: msg VC == local VC, the kvs values better be the same
    elif msg_key_vc == local_key_vc:
        if val != SharedData.kvstore[key]:
            raise ValueError("!!!!!!!!!!!!!!!!!!!!!!!For update, vector clocks are the same but values aren't. This is really bad!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    
    # Case 2: msg || local AND local wins the tiebreaker, don't deliver
    elif msg_key_vc.isConcurrent(local_key_vc) and msg_key_vc.concurrent_break_ties(local_key_vc) == local_key_vc:
        return JSONResponse({
            "message": f"Update did not occur - local VC {local_key_vc} || message's VC {msg_key_vc}, but local wins tiebreaker."
        })

    # Case 3: msg || local & msg wins, local -> msg, or msg == local, deliver
    else:
        # Merge client metadata with our node's key's metadata, along with KVS data

        self_dependencies = SharedData.causal_data.get(key, {})
        util.update_metadata(self_dependencies, causal_metadata, SharedData.kvstore, {key: val}, key)
        SharedData.causal_data[key] = self_dependencies
        print("After update, causal metadata is", util.server_metadata_to_dict(SharedData.causal_data))
        print(SharedData.kvstore[key])
        return JSONResponse({"message": f"Replicated data with key {key} and value {val}"})
