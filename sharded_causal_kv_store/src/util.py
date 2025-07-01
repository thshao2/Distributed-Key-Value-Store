from shared_data import SharedData
from packages.vector_clock import VectorClock

import copy

def in_current_view():
    """ Helper to check if this node is in current_view. """
    for shard in SharedData.current_view:
        for node in SharedData.current_view[shard]:
            if node["id"] == int(SharedData.NODE_IDENTIFIER):
                return True
    return False

def get_node_address_by_id(node_id):
    """ Helper to get the IP address of a node id. """
    for shard in SharedData.current_view:
        for node in shard:
            if node["id"] == node_id:
                return node["address"]
    return None

def extract_ids(view):
    """ Helper to get all node ids in the current view and put them in a list."""
    ids = []
    for shard in view:
        for node in SharedData.current_view[shard]:
            ids.append(int(node["id"]))
    return ids

def list_view_attributes(data):
    """
    Returns a list of all attributes (keys) from the "view" dictionary.
    """
    view = data.get("view", {})
    return [key for key in view.keys()]

def find_shard_by_node(view, node_id):
    """
    Returns the shard name (key from the "view" dictionary) where the node with the given id is found.
    It returns None if the node id is not found.
    """
    for shard, nodes in view.items():
        for node in nodes:
            if node.get("id") == node_id:
                return shard  # returning the shard key as stored in the data (e.g., "Shard1")
    return None

def get_nodes_by_shard(data, shard_name):
    """
    Given the data object and a shard name, returns a list of node objects in that shard.
    If the shard is not found, it returns an empty list.
    """
    view = data.get("view", {})
    if view == {}: # In case they meant to pass in the view without nesting it in "data"
        view = data

    return view.get(shard_name, [])

def key_in_current_shard(key: str) -> bool:
    """
    Returns True if the key belongs in the current shard.
    """
    return SharedData.current_shard == SharedData.hash_circle.get_shard_for_key(key)

def update_client_metadata(client: dict, server: dict):
    """ Helper to update client causal metadata from client and server causal key -> vector clock recordings. """
    newData = copy.deepcopy(client)
    for key in server.keys():
        if (key not in newData):
            newData[key] = server[key]
            continue
        client_vc: VectorClock = newData[key]
        server_vc: VectorClock = server[key]
        if (server_vc > client_vc):
            newData[key] = server[key]

    return newData

def causal_data_to_dict(causal_data: dict[str, VectorClock]):
    """Helper to convert causal_data into json.
    
    causal_data is in the format of {<key: str>: <VectorClock>}.
    
    Convert to {<key: str>: <<node_id: str>: <int>>}

    Ex return:
        {
            "hello": {"0": 1, "1": 1}
        }
    """
    ret = {}
    for key in causal_data:
        if type(causal_data[key]) == VectorClock:
            ret[key] = causal_data[key].to_dict()
        else:
            ret[key] = causal_data[key]
    return ret

def dict_to_causal_data(causal_data_dict):
    """opposite of causal_data_to_dict() (Deserialization)"""
    ret = {}
    for key in causal_data_dict:
        ret[key] = VectorClock(list(causal_data_dict[key].keys()), causal_data_dict[key])
    return ret

def update_metadata_view(self_data: dict, target_data: dict):
    """
    update vector clocks of self_data to 
    have all the keys in target_data's clocks
    """
    for key, target_clock in target_data.items():
        self_clock = self_data.get(key, VectorClock([]))
        self_clock.update_view(target_clock)
    # return self_data

def update_metadata(self_data, target_data, self_kvs, target_kvs, target_key):
    """
    Takes max of two clocks and key value associated with it
    Additional Note:
    put self_data first then target_data to 
    ensure all values from target are transferred to self
    """
    update_metadata_view(self_data, target_data)
    update_metadata_view(target_data, self_data)

    for key, target_clock in target_data.items():
        self_clock = self_data.get(key, VectorClock(extract_ids(SharedData.current_view)))
        # if dependency is the target key, use target's value if its clock is higher or wins tiebreaker
        if key == target_key:
            if (target_clock > self_clock or self_clock.concurrent_break_ties(target_clock) == target_clock):
                self_kvs[key] = target_kvs[key]
        # update dependencies to pairwise max of the two clocks
        self_data[key] = self_clock.pairwise_max(target_clock)

def dict_to_server_metadata(server_metadata):
    """
    convert dict to server metadata (key: {key : clock}) for http requests
    """
    return {key: dict_to_causal_data(value) for key, value in server_metadata.items()} 

def server_metadata_to_dict(server_metadata):
    """
    convert server metadata (key: {key : clock}) to dict for http requests
    """
    return {key: causal_data_to_dict(value) for key, value in server_metadata.items()}

def assemble_get_all_metadata_dict(server_metadata, client_metadata):
    """Assembles the get all metadata dictionary to return to client.

    Call this for get all once the hanging is done. Go thorugh each key, and take the pairwise max of 
    server_metadata[key][key] and client_metadata[key].

    Assume there are no keys that exist in client_metadata but not in server_metadata (since this is called AFTER we ensure causal ordering).

    Server_metadata will always be SharedData.causal_data.
    """
    updated_metadata = {}
    for key in server_metadata:
        local_key_vc = server_metadata.get(key, {}).get(key)
        if not local_key_vc:
            raise ValueError("Inside assemble_get_all_metadata_dict, server_metadata[key][key] does not exist, this is really bad.")

        # If client also has a metadata field - determine which one to return as updated
        client_key_vc = client_metadata.get(key)
        if client_key_vc:
            # FATAL CASE: client's VC > server VC or client VC wins tiebreaker, we shouldn't have stopped hanging
            if local_key_vc < client_key_vc or (local_key_vc.isConcurrent(client_key_vc) and local_key_vc.concurrent_break_ties(client_key_vc) == client_key_vc):
                raise Exception("FATAL ERROR in assemble_get_all_metadata_dict: should of kept hanging because server's value is not causally consistent")
            
        # Use the server's VC as the updated metadata
        updated_metadata[key] = local_key_vc

    # Serialize to json and return
    json_updated_metadata = causal_data_to_dict(updated_metadata)
    return json_updated_metadata
    