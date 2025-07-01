from shared_data import SharedData

def in_current_view():
    """ Helper to check if this node is in current_view. """
    for node in SharedData.current_view:
        if node["id"] == int(SharedData.NODE_IDENTIFIER):
            return True
    return False

def get_primary_id():
    """ Define the primary as the node with the smallest id. """
    if not SharedData.current_view:
        return None
    return min(node["id"] for node in SharedData.current_view)

def is_primary():
    """ Helper to check if this node has the smallest id in the current_view (making it the primary). """
    return int(SharedData.NODE_IDENTIFIER) == get_primary_id()

def get_node_address_by_id(node_id):
    """ Helper to get the IP address of a node id. """
    for node in SharedData.current_view:
        if node["id"] == node_id:
            return node["address"]
    return None