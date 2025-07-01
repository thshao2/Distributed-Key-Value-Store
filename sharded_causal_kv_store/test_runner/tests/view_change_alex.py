"""Credits: Alex's testing repo"""

from time import sleep

from ..containers import ClusterConductor
from ..testcase import TestCase
from ..util import log, Logger
from .helper import KVSMultiClient, KVSTestFixture

# starts off with 2 nodes in a partition. Nodes will be given writes
# after enough time is given for parity, drop one node from the view
# and then add a new node to the view.
def view_change_basic(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1, 2]))
        alice = KVSMultiClient(fx.clients, "alice", log)
        bob = KVSMultiClient(fx.clients, "bob", log)
        
        log("\n> TEST VIEW CHANGE BASIC 1")
        
        # create a view for only [0,1] partition, send to 0,1
        conductor.create_partition([0, 1], "p0")
        conductor.create_partition([2], "p1")
        fx.send_view(0, conductor.get_partition_view("p0"))
        fx.send_view(1, conductor.get_partition_view("p0"))
        
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()
        
        # send some writes to main view
        r = alice.put(0, "a", "1")
        assert r.ok, f"expected 200 for put, got {r.status_code}"
        
        r = bob.put(1, "a", "2")
        assert r.ok, f"expected 200 for get, got {r.status_code}"
        
        r = alice.put(1, "b", "3")
        assert r.ok, f"expected 200 for put, got {r.status_code}"
        
        r = bob.put(0, "c", "4")
        assert r.ok, f"expected 200 for get, got {r.status_code}"
        
        # wait for convergence
        sleep(10)
        
        # drop node 1 from the view
        conductor.create_partition([0], "p2")
        conductor.create_partition([1], "p3") # isolate this
        fx.send_view(0, conductor.get_partition_view("p0"))
        
        # add node 2 to the view
        conductor.create_partition([0, 2], "p4")
        fx.send_view(0, conductor.get_partition_view("p4"))
        fx.send_view(2, conductor.get_partition_view("p4"))
        
        log("\n> NETWORK TOPOLOGY after view changes")
        conductor.describe_cluster()
        
        r = alice.put(0, "a", "5")
        assert r.ok, f"expected 200 for put, got {r.status_code}"
        
        r = bob.put(2, "e", "6")
        assert r.ok, f"expected 200 for put, got {r.status_code}"
        
        # wait for convergence
        sleep(10)
        
        KVS_state_0 = alice.get_all(0)
        assert KVS_state_0.ok, f"expected 200 for get, got {KVS_state_0.status_code}"
        
        KVS_state_1 = bob.get_all(2)
        assert KVS_state_1.ok, f"expected 200 for get, got {KVS_state_1.status_code}"
        
        assert KVS_state_0.json()["items"] == KVS_state_1.json()["items"], f"expected KVS states to be equal, got {KVS_state_0.json()} and {KVS_state_1.json()}"
        
        return True, "ok"
    
VIEW_CHANGE_TESTS = [
    TestCase("view_change_basic", view_change_basic),
]
