from typing import List, Dict, Any, Optional
import requests

from ..containers import ClusterConductor
from ..util import log
from ..kvs_api import KVSClient
from ..testcase import TestCase

from .helper import KVSTestFixture, KVSMultiClient
import time


# If the nodes were partitioned and they had concurrent writes, once partition heals, they eventually become consistent
# This mostly tests the tiebreaking during a view change, not the gossip or broadcast update
def test_eventual_partition_heals_view_change(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients, name="mc")

        log("\n> TEST PARTITION HEALS VIEW CHANGE")

        # isolate node 0
        conductor.create_partition([0], "p0")
        fx.send_view(0, conductor.get_partition_view("p0"))

        # isolate node 1
        conductor.create_partition([1], "p1")
        fx.send_view(1, conductor.get_partition_view("p1"))

        # Put a value to node 0
        r = mc.put(0, "test1", "hello")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Put a different value to node 1
        r = mc.put(1, "test1", "world")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Heal partition
        conductor.create_partition([0, 1], "join")
        fx.broadcast_view(conductor.get_full_view())

        # Wait for 10 second to allow the nodes to reach consistency
        time.sleep(8)

        # Get value from both requests and ensure eventual consistency
        r0 = mc.get(0, "test1")
        assert r0.status_code == 200, f"expect 200 for get to node 0, got {r0.status_code}"
        r1 = mc.get(1, "test1")
        assert r1.status_code == 200, f"expect 200 for get to node 1, got {r1.status_code}"
        assert r0.json()["value"] == r1.json()["value"], f"node 0 and 1 don't have same values"
        assert r0.json()["value"] == "world", f'node 0 is not causally consistent, kept old value {r0.json()["value"]}'

        # return score/reason
        return True, "ok"

def test_eventual_no_partitions(conductor: ClusterConductor):
    """Ensure data is synced across nodes without partitions.

    This tests that the broadcast update and gossip protocol works correctly.
    """
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients, name="mc")

        log("\n> TEST EVENTUAL NO PARTITIONS")

        # Put a value to node 1
        r = mc.put(1, "test1", "hello")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Put a different value to node 0
        r = mc.put(0, "test1", "world")

        # Wait for 10 second to allow the nodes to reach consistency
        time.sleep(8)

        # Get value from both requests and ensure eventual consistency
        r0 = mc.get(0, "test1")
        assert r0.status_code == 200, f"expect 200 for get to node 0, got {r0.status_code}"
        r1 = mc.get(1, "test1")
        assert r1.status_code == 200, f"expect 200 for get to node 1, got {r1.status_code}"
        assert r0.json()["value"] == r1.json()["value"]

        # return score/reason
        return True, "ok"

def test_eventual_gossip(conductor: ClusterConductor): 
    """Ensure a node can be caught up when there's an unknown partition.

    This will test to see if our gossip protocol works correctly.
    
    Setup: 
        1. View contains [n1 n2 n3], but n3 is unknownly partitioned away from n1 and n2.
        2. Send puts to n1 and n2.
        3. Unpartition n3 and check for eventual consistency.
    """
    with KVSTestFixture(conductor, node_count=3) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients, name="mc")

        log("\n> TEST EVENTUAL GOSSIP")

        # Isolate node 3 from the other two nodes.
        conductor.create_partition([2], "p")

        # Put different values to node 0 & 1
        r = mc.put(0, "test1", "hello")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"
        r = mc.put(1, "test1", "world")
        assert r.status_code == 200, f"expected 200 for put, got {r.status_code}"

        # Reconnect n3 with the other 2
        conductor.create_partition([2], "base")

        # Wait for 10 second to allow the nodes to reach consistency
        time.sleep(8)

        # Get value from all requests and ensure eventual consistency
        r0 = mc.get(0, "test1")
        assert r0.status_code == 200, f"expect 200 for get to node 0, got {r0.status_code}"
        r1 = mc.get(1, "test1")
        assert r1.status_code == 200, f"expect 200 for get to node 1, got {r1.status_code}"
        r2 = mc.get(2, "test1")
        assert r2.status_code == 200, f"expect 200 for get to node 2, got {r2.status_code}"
        assert r0.json()["value"] == r1.json()["value"]
        assert r1.json()["value"] == r2.json()["value"]

        return True, "ok"



EVENTUAL_CONSISTENCY_TESTS = [
    TestCase("test_eventual_partition_heals_view_change", test_eventual_partition_heals_view_change),
    TestCase("test_eventual_no_partitions", test_eventual_no_partitions),
    TestCase("test_eventual_gossip", test_eventual_gossip)
]
