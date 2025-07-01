from flask import Blueprint, request, jsonify
from shared_data import SharedData
from helper import ReqHelper
from packages.fifo import FifoDelivery

import requests

import util


data_api = Blueprint('data_api', __name__)

@data_api.route('/data', methods=['GET'])
def get_all_data():
    if (SharedData.role == "primary"):
        return jsonify(SharedData.kvstore), 200
    
    elif(SharedData.role == "backup"):
        # Forward to Primary
        primary_id = util.get_primary_id()
        primary_addr = util.get_node_address_by_id(primary_id)

        # Proxy the Request
        try:
            r = requests.get(f"http://{primary_addr}/data")
            return (r.text, r.status_code, r.headers.items())
        except requests.exceptions.RequestException:
            return jsonify(error="Error in Proxying to Primary"), 500
    
    # Catch-all if role is not set or recognized.
    else:
        return jsonify(error=f"Node {SharedData.NODE_IDENTIFIER} role is not set or unrecognized"), 503

@data_api.route('/data/<key>', methods=['GET'])
def get_data(key):
    if (SharedData.role == "primary"):
        headers = ReqHelper.create_req_headers(FifoDelivery.primary_curr_num)
        if key not in SharedData.kvstore:
            return jsonify(error='Key Not Found'), 404, headers
        
        return jsonify(value=SharedData.kvstore[key]), 200, headers
    elif (SharedData.role == "backup"):
        # Forward to Primary
        primary_id = util.get_primary_id()
        primary_addr = util.get_node_address_by_id(primary_id)

        # Proxy the Request
        r = requests.get(f"http://{primary_addr}/data/{key}")
        return (r.text, r.status_code, r.headers.items())
    
    # Catch-all if role is not set or recognized.
    else:
        return jsonify(error=f"Node {SharedData.NODE_IDENTIFIER} role is not set or unrecognized"), 503

@data_api.route('/replicate', methods=['POST'])
def replicate():
    """
    Internal endpoint for primary backup replication.
    Expects JSON: {
      "operation": "PUT" or "DELETE",
      "key": "<key>",
      "value": "<val>" # only for PUT
    }
    """
    data = request.get_json()
    operation = data["operation"]
    key = data["key"]

    # Extract headers
    sender_node_id = ReqHelper.extract_node_id_header(request)
    msg_num = ReqHelper.extract_msg_num_header(request)
    
    # Deliver based on FIFO delivery
    FifoDelivery.can_deliver(sender_node_id, msg_num)

    if operation == "PUT":
        val = data["value"]
        SharedData.kvstore[key] = val
        print(SharedData.kvstore[key])
        FifoDelivery.finished_delivering(sender_node_id)
        return jsonify(message=f"Replicated data with key {key} and value {val}"), 200
    elif operation == "DELETE":
        SharedData.kvstore.pop(key, None)
        FifoDelivery.finished_delivering(sender_node_id)
        return jsonify(message=f"Deleted {key} from backup ${util.get_node_address_by_id(SharedData.NODE_IDENTIFIER)}"), 200

def replicate_to_backup(backup_address, operation, headers, key, value=None):
    """
    Send a request to the backup's /replicate endpoint. Return True if successful.
    """
    payload = {
        "operation": operation,
        "key": key
    }

    if operation == "PUT":
        payload["value"] = value
    
    try:
        print(f"http://{backup_address}/replicate", payload)
        r = requests.post(f"http://{backup_address}/replicate", json=payload, headers=headers)
        print(r.status_code)
        return r.status_code == 200
    except requests.exceptions.RequestException as error:
        print(f"ERROR {error}")
        return False

@data_api.route('/data/<key>', methods=['PUT'])
def put_data(key):
    if not request.is_json or "value" not in request.get_json() or not isinstance(request.get_json()["value"], str):
        return jsonify(error='Missing or invalid JSON body. Expected { "value": "string" }'), 400

    data = request.get_json()

    if (SharedData.role == "primary"):        
        value = data['value']

        # Get a message number to attach to reqs to backup - FIFO Delivery
        msg_num = FifoDelivery.get_new_msg_num()
        headers = ReqHelper.create_req_headers(msg_num)

        # Replicate to Backups
        for node in SharedData.current_view:
            if node["id"] != int(SharedData.NODE_IDENTIFIER):
                ack = replicate_to_backup(str(node["address"]), "PUT", headers, str(key), str(data["value"]))
                if not ack:
                    return jsonify(description="Replication failed for node: " + node["address"]), 500
        
        # Primary needs to deliver in order based on what it sent backup
        FifoDelivery.primary_can_deliver(msg_num)
        key_in_kvs = key in SharedData.kvstore
        SharedData.kvstore[key] = value # Commit Point
        FifoDelivery.primary_finished_delivering()

        if not key_in_kvs:
            return jsonify(message='Key created successfully.'), 201, headers
        else:
            return jsonify(message='Key updated successfully.'), 200, headers
        
    elif (SharedData.role == "backup"):
        # Forward to Primary
        primary_id = util.get_primary_id()
        primary_addr = util.get_node_address_by_id(primary_id)

        # Proxy the Request
        r = requests.put(f"http://{primary_addr}/data/{key}", json=data)
        return (r.text, r.status_code, r.headers.items())
    
    # Catch-all if role is not set or recognized.
    else:
        return jsonify(error=f"Node {SharedData.NODE_IDENTIFIER} role is not set or unrecognized"), 503

@data_api.route('/data/<key>', methods=['DELETE'])
def delete_data(key):

    if (SharedData.role == "primary"):
        # print("Inside delete for primary: about to mutex lock to check if key exist")
        # # Ensure no other thread is accessing the kvstore
        # # This is a dummy call to the FifoDelivery function (condition is always true) -  using it as a normal lock
        # FifoDelivery.primary_can_deliver(FifoDelivery.primary_curr_num)
        # if key not in SharedData.kvstore:
        #     FifoDelivery.primary_curr_num -= 1 # next function call increments it, so this cancels the effect
        #     FifoDelivery.primary_finished_delivering()
        #     return jsonify(error='Key Not Found'), 404
        # FifoDelivery.primary_curr_num -= 1 # next function call increments it, so this cancels the effect
        # FifoDelivery.primary_finished_delivering()
        
        # Get a message number to attach to reqs to backup - FIFO Delivery
        msg_num = FifoDelivery.get_new_msg_num()
        headers = ReqHelper.create_req_headers(msg_num)

        # Replicate to Backups
        for node in SharedData.current_view:
            if node["id"] != int(SharedData.NODE_IDENTIFIER):
                ack = replicate_to_backup(node["address"], "DELETE", headers, key)
                if not ack:
                    return jsonify(description="Replication failed for node: " + node["address"]), 500        

        # Primary needs to deliver in order based on what it sent backup
        FifoDelivery.primary_can_deliver(msg_num)
        print("Will be handling delete request from", request.url)
        deleted_val = SharedData.kvstore.pop(key, None) # commit point
        print("deleted_val =", deleted_val)
        FifoDelivery.primary_finished_delivering()

        # Check if key exsited at the moment of deletion
        if not deleted_val:
            return jsonify(message=f'Key Not Found.'), 404, headers
        else:
            return jsonify(message=f'Key {key} deleted successfully.'), 200, headers
    
    elif (SharedData.role == "backup"):
        # Forward to Primary
        primary_id = util.get_primary_id()
        primary_addr = util.get_node_address_by_id(primary_id)

        # Proxy the Request
        r = requests.delete(f"http://{primary_addr}/data/{key}")
        return (r.text, r.status_code, r.headers.items())

    # Catch-all if role is not set or recognized.
    else:
        return jsonify(error=f"Node {SharedData.NODE_IDENTIFIER} role is not set or unrecognized"), 503