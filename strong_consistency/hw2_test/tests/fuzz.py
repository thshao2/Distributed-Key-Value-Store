"""Test adapted from assignment 1 (partly inspired by Laurel Willey).

This test will start up 6 client threads (2 for each operation) and 4 nodes.
For each client thread, it'll send many requests to the distributed system (random 
node every request) and maintain an audit log. After all the clients are done sending
their requests, we run a conosistency violation detector to check for any weird response orders.
"""

from typing import List, Dict, Any, Optional
import requests

from ..containers import ClusterConductor
from ..util import log
from ..kvs_api import KVSClient
from ..testcase import TestCase

from .helper import KVSTestFixture, KVSMultiClient

import requests
import threading
import time
import random
import string
import json

KEY = "testkey"
ITERATIONS = 30
FILENAME = "output.txt"

class MultiClientTest:
    def __init__(self, fx: KVSTestFixture, mc: KVSMultiClient):
        self.fx = fx
        self.mc = mc

    def multi_client_send_requests(self, get_threads=2, put_threads=2, del_threads=2):
        def get_node_url():
            return random.choice(self.fx.clients).base_url
        def put_values(thread_id):

                # Gen random alphabet letters to PUT
                random_put_values = [random.choice(string.ascii_letters) for _ in range(ITERATIONS)]

                # PUT each value in list
                for value in random_put_values:
                    node_url = get_node_url()
                    # Make request and get response
                    put_response = requests.put(
                        f"{node_url}/data/{KEY}",
                        data=json.dumps({"value": value}),
                        headers={"Content-Type": "application/json"},
                    )
                    msg_num = put_response.headers['Msg-Num']

                    # Updated key
                    if put_response.status_code == 200:
                        outfile.write(f"{msg_num} T{thread_id} {node_url} PUT 200 {value}\n")
                        print(f"{msg_num} T{thread_id} {node_url} PUT 200 {value}")

                    # Created key
                    elif put_response.status_code == 201:
                        outfile.write(f"{msg_num} T{thread_id} {node_url} PUT 201 {value}\n")
                        print(f"{msg_num} T{thread_id} {node_url} PUT 201 {value}")

                    # Err
                    else:
                        outfile.write(f"{msg_num} T{thread_id} {node_url} PUT {put_response.status_code}\n")
                        print(f"{msg_num} T{thread_id} {node_url} PUT {put_response.status_code}")
                    
                    # Sleep for random time <= 1s
                    time.sleep(random.uniform(0, 0.5))

        def get_values(thread_id):
            # Perform gets
            for _ in range(ITERATIONS):
                node_url = get_node_url()
                # Make request and get response
                get_response = requests.get(f"{node_url}/data/{KEY}")
                msg_num = get_response.headers['Msg-Num']

                # Key found
                if get_response.status_code == 200:
                    get_response_val = get_response.json().get('value')
                    outfile.write(f"{msg_num} T{thread_id} {node_url} GET 200 {get_response_val}\n")
                    print(f"{msg_num} T{thread_id} {node_url} GET 200 {get_response_val}")

                # Key not found (does not exist)
                else:
                    outfile.write(f"{msg_num} T{thread_id} {node_url} GET {get_response.status_code}\n")
                    print(f"{msg_num} T{thread_id} {node_url} GET {get_response.status_code}")
                
                # Sleep for random time <= 1s
                time.sleep(random.uniform(0, 0.5))

        def delete_values(thread_id):
            # Perform 6 DELETEs
            for _ in range(ITERATIONS//2):
                node_url = get_node_url()
                # Make request and get response
                delete_response = requests.delete(f"{get_node_url()}/data/{KEY}")
                msg_num = delete_response.headers['Msg-Num']

                outfile.write(f"{msg_num} T{thread_id} {node_url} DELETE {delete_response.status_code}\n")
                print(f"{msg_num} T{thread_id} {node_url} DELETE {delete_response.status_code}")

                time.sleep(random.uniform(0, 0.5))

        try:
            outfile = open(FILENAME, "w")

            # make all the threads and add to a list
            thread_num = 1
            threads = []
            for i in range(get_threads):
                threads.append(threading.Thread(target=get_values, args=(thread_num,)))
                thread_num += 1
            for i in range(put_threads):
                threads.append(threading.Thread(target=put_values, args=(thread_num,)))
                thread_num += 1
            for i in range(del_threads):
                threads.append(threading.Thread(target=delete_values, args=(thread_num,)))
                thread_num += 1

            # Randomize the order of thread starting
            random.shuffle(threads)

            for t in threads:
                t.start()

            # Join in same order as start
            for t in threads:
                t.join()
            outfile.close()

        except requests.RequestException as e:
            print(f"TEST ERROR: {e}")
            exit(1)
    

"""Open audit log and sort based on message number.

Returns:
- List of List of Tokens, Each entry representing a response 
- Ex: [
        ['10', 'T3', 'http://localhost:9003', 'PUT', '200', 'T'],
        ['2', 'T3', 'http://localhost:9003', 'DELETE', '200']
      ]
- Each entry has the value of:
    [<message number>, <thread number>, <request dest>, <method> <status code>, <optional value for GET and PUT>]
"""
def organize_audit_log():
  infile = open(FILENAME, "r")
  lines = infile.readlines()
  infile.close()
  
  expected_val = None

  method_priority = {"GET": 1, "PUT": 2, "DELETE": 2} # GET > PUT or DELETE when sorting msgs with same msg_num
  line_tokens = [line.strip().split(' ') for line in lines]
  line_tokens = sorted(line_tokens, key=lambda x : (int(x[0]), method_priority[x[3]])) # sort by msg_num then method

  return line_tokens

def check_FIFO(line_tokens):
    count_by_msg_num = {} # key = msg_num, val = count (only for PUTs and DELETEs)
    for tokens in line_tokens:
        msg_num = tokens[0]
        method = tokens[3]
        if method == 'PUT' or method == 'DELETE':
            if msg_num not in count_by_msg_num:
                count_by_msg_num[msg_num] = 0
            count_by_msg_num[msg_num] += 1


    # Get all keys with more than one PUTs or DELETEs
    keys_with_large_values = [key for key, value in count_by_msg_num.items() if value > 1]
    
    assert len(keys_with_large_values) == 0, f"FIFO Delivery is violated - message numbers {keys_with_large_values} has multiple conflicting operations"

def check_strong_consistency(line_tokens):
  expected_val = None
  # Go through each entry
  count = 0
  for tokens in line_tokens:
    count += 1
    # Gather information from the tokens in each line
    msg_num = tokens[0]
    thread_num = tokens[1]
    node_num = tokens[2]
    method = tokens[3]
    status_code = int(tokens[4])
    value = tokens[5] if len(tokens) == 6 else None


    if method == "PUT":
      # Case 1: key doesn't exist, expect a 201.
      if not expected_val:
        assert status_code == 201, f"Violation for line {count} ({tokens}) - Expected a 201 PUT"
        expected_val = value
      # Case 2: key exist, expect a 200.
      else:
        assert status_code == 200, f"Violation for line {count} ({tokens}) - Expected a 200 PUT"
        expected_val = value
    
    elif method == "GET":
      # Case 1: key exist, expect a 200 with the correct value.
      if expected_val:
        assert status_code == 200, f"Violation for line {count} ({tokens}) - Expected a 200 GET"
        assert value == expected_val, f"Violation for line {count} ({tokens}) - Expected value to be {expected_val}"
      # Case 2: key doesn't exist, expect a 404.
      else:
        assert status_code == 404, f"Violation for line {count} ({tokens}) - Expected a 404 GET"
      
    elif method == "DELETE":
      # Case 1: key exist, expect a 200.
      if expected_val:
        assert status_code == 200, f"Violation for line {count} ({tokens}) - Expected a 200 DELETE"
        expected_val = None
      # Case 2: key doesn't exist, expect a 404.
      else:
        assert status_code == 404, f"Violation for line {count} ({tokens}) - Expected a 404 DELETE"
        expected_val = None

def test_multi_client(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=4) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)

        MultiClientTest(fx, mc).multi_client_send_requests(del_threads=1)

        # Organize audit log output
        line_tokens = organize_audit_log()

        # Ensure FIFO Delivery (one PUT or DELETE per message number)
        check_FIFO(line_tokens)
        print("FIFO Delivery satisfied.")

        # Check the validity of all the GETs
        check_strong_consistency(line_tokens)
        print("No violations!")


        # Isolate all nodes and check if they have the same state
        val = None
        for i in range(4):
            conductor.create_partition([i], f"p{i}")
            fx.send_view(i, conductor.get_partition_view(f"p{i}"))
            # Make sure all values are the same
            get_response = mc.get(i, KEY)
            get_val = get_response.json().get("value")
            print(i, get_val)
            if i != 0:
              assert val == get_val
            val = get_val
    
    return True, "ok"

FUZZ_TESTS = [
    TestCase("test_multi_clients", test_multi_client),
]