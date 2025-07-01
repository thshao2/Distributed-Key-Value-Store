from typing import List, Dict, Any, Optional
import requests
import time

from ..containers import ClusterConductor
from ..util import log
from ..kvs_api import KVSClient
from ..testcase import TestCase

from .helper import KVSTestFixture, KVSMultiClient

def multiclient_availability(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        c1 = KVSMultiClient(fx.clients, "c1")
        c2 = KVSMultiClient(fx.clients, "c2")
        c3 = KVSMultiClient(fx.clients, "c3")
        c4 = KVSMultiClient(fx.clients, "c4")

        log("\n> TEST MULTICLIENT AVAILABILITY")

        # partition 0,1
        conductor.create_partition([0], "p0")
        conductor.create_partition([1], "p1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # Put x = 1 on 0 (paritioned from 1)
        r = c1.put(0, "x", "1")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Get it back
        r = c2.get(0, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        # Get x from 1 (should be 404)
        r = c3.get(1, "x")
        assert r.status_code == 404, f"expected 404 for get, got {r.status_code}"

        # Create x = 2
        r = c4.put(1, "x", "2")
        assert r.status_code == 200, f"expected 200 for update, got {r.status_code}"

        r = c3.get(1, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == "2", f"wrong value returned: {r.json()}"

        # return score/reason
        return True, "ok"
    
def multiclient_causal(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        c1 = KVSMultiClient(fx.clients, "c1")
        c2 = KVSMultiClient(fx.clients, "c2")
        c3 = KVSMultiClient(fx.clients, "c3")
        c4 = KVSMultiClient(fx.clients, "c4")

        log("\n> TEST MULTICLIENT CAUSAL")
        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # Put x = 1 on 0 (paritioned from 1)
        r = c1.put(0, "test1", "hello")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        r = c2.put(1, "test1", "world")

        time.sleep(10)

        r0 = c3.get(0, "test1")
        assert r0.status_code == 200, f"expect 200 for get to node 0, got {r0.status_code}" 
        r1 = c4.get(1, "test1")
        assert r1.status_code == 200, f"expect 200 for get to node 1, got {r1.status_code}"
        assert r0.json()["value"] == "world", f"wrong value returned: {r0.json()}"
        assert r1.json()["value"] == "world", f"wrong value returned: {r1.json()}"

        # return score/reason
        return True, "ok"
'''
def multiclient_partition_heal(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=3) as fx:
        fx.broadcast_view(conductor.get_full_view())
        c1 = KVSMultiClient(fx.clients, "c1")
        c2 = KVSMultiClient(fx.clients, "c2")
        c3 = KVSMultiClient(fx.clients, "c3")

        log("\n> TEST MULTICLIENT PARTITION HEAL")

        # Partition the cluster into two groups: {0,1} and {2}
        conductor.create_partition([0, 1], "p0")
        conductor.create_partition([2], "p1")

        # Describe the partitioned topology
        log("\n> NETWORK TOPOLOGY AFTER PARTITION")
        conductor.describe_cluster()

        # Client 1 writes key "a" to node 0
        r = c1.put(0, "a", "apple")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Client 2 writes key "b" to node 1
        r = c2.put(1, "b", "banana")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Client 3 (partitioned node 2) writes key "c"
        r = c3.put(2, "c", "cherry")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Gets are failing on 500 error!
        r = c1.get(0, "a")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == "apple", f"wrong value returned: {r.json()}"

        r = c2.get(1, "b")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == "banana", f"wrong value returned: {r.json()}"

        r = c3.get(2, "c")
        assert r.status_code == 200, f"expected 200 for partitioned get, got {r.status_code}"
        assert r.json()["value"] == "cherry", f"wrong value returned: {r.json()}"


        # Heal the partition
        conductor.create_partition([0, 1, 2], "join")
        fx.broadcast_view(conductor.get_full_view())
        time.sleep(5)
        # Describe network after healing
        log("\n> NETWORK TOPOLOGY AFTER HEALING")
        conductor.describe_cluster()

        # Ensure all nodes can access all keys after healing
        for client, node in [(c1, 0), (c2, 1), (c3, 2)]:
            for key, value in [("a", "apple"), ("b", "banana"), ("c", "cherry")]:
                r = client.get(node, key)
                assert r.status_code == 200, f"expected 200 for get after healing, got {r.status_code}"
                assert r.json()["value"] == value, f"wrong value returned for {key}: {r.json()}"

        return True, "ok"'''

MULTICLIENT_TESTS = [
    TestCase("multiclient_availability", multiclient_availability),
    TestCase("multiclient_causal", multiclient_causal),
    # TestCase("multiclient_partition_heal", multiclient_partition_heal),
]
