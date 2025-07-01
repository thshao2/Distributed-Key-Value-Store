from ..containers import ClusterConductor
from ..util import log
from ..testcase import TestCase

from .helper import KVSTestFixture, KVSMultiClient

from urllib.parse import urljoin


import threading
import time
import random
import requests

KEY = "testkey"
ITERATIONS = 30
FILENAME = "output.txt"

def view_change_in_progress(conductor: ClusterConductor):
    stop = threading.Event()
    with KVSTestFixture(conductor, node_count=3) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)

        # initialize kvstore with a value
        r = mc.put(0, "key1", "value1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        acked = False
        
        def async_put():
            # use random delay of 0-1 sec on put
            delay = random.random() + 1
            print("put delay: ", delay)
            time.sleep(delay)
            try:
                r = mc.put(0, "key1", "value2")
                nonlocal acked
                acked = True if r.status_code == 200 else False
            except requests.exceptions.ConnectionError as e:
                log(f"async_put: ConnectionError caught (likely due to view change!): {e}")
            except Exception as e:
                log(f"async_put: Unexpected exception: {e}")
            
            
        def async_view_change():
            delay = random.random() +  1
            print("put delay: ", delay)
            time.sleep(delay)
            conductor.create_partition([1,2], "new_view")
            fx.send_view(1, conductor.get_partition_view("new_view"))
            fx.send_view(2, conductor.get_partition_view("new_view"))
        
        put_request = threading.Thread(target=async_put)
        view_change = threading.Thread(target=async_view_change)
        
        # perform put and view change concurrently
        put_request.start()
        view_change.start()
        view_change.join()
        put_request.join(timeout=0.2)
        
        if not acked:
            # no ack returned, verify that kvs still works after view change
            r = mc.get(1, "key1")
            assert r.status_code == 200, "GET key1 returned non-success code"
            r = mc.get(2, "key1")
            assert r.status_code == 200, "GET key1 returned non-success code"
            log("View change correct with no ACK received from PUT request!")
        else:
            # ack returned, verify that kvs updated values correctly
            r = mc.get(1, "key1")
            assert r.json()["value"] == "value2", "did not update node 1 with value 2"
            r = mc.get(2, "key1")
            assert r.json()["value"] == "value2", "did not update node 1 with value 2"
            log("Worked with ACK received and view change!")

        return True, "ok"

VIEW_TESTS = [
  TestCase("view_change_in_progress", view_change_in_progress),
]
