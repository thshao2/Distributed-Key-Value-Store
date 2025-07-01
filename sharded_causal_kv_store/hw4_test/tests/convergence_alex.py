"""Credits: Alex's testing repo"""

from time import sleep

from ..containers import ClusterConductor
from ..testcase import TestCase
from ..util import log, Logger
from .helper import KVSMultiClient, KVSTestFixture
import time

CONVERGENCE_TIME = 10

def convergence_concurrent_basic_1(conductor: ClusterConductor, dir, log: Logger):
    global CONVERGENCE_TIME
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1, 2]))
        fx.broadcast_view(conductor.get_shard_view())
        c1 = KVSMultiClient(fx.clients, "c1", log)
        c2 = KVSMultiClient(fx.clients, "c2", log)
        c3 = KVSMultiClient(fx.clients, "c3", log)
        c4 = KVSMultiClient(fx.clients, "c4", log)

        log("\n> TEST CONVERGENCE CONCURRENT BASIC 1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # Put x = 0 on 0
        r = c1.put(0, "x", "0")
        assert r.ok, f"expected successful response for new key, got {r.status_code}"

        # Put x = 1 on 1
        r = c2.put(1, "x", "1")
        assert r.ok, f"expected successful response, got {r.status_code}"

        # Put x = 2 on 2
        r = c3.put(2, "x", "2")
        assert r.ok, f"expected successful response, got {r.status_code}"

        # Wait 10 seconds (the deadline for the eventual consistency)
        log(f"\n> WAITING {CONVERGENCE_TIME} SECONDS")
        time.sleep(CONVERGENCE_TIME)

        # Get them back, they should all match.
        r = c4.get(0, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        result = r.json()["value"]

        r = c4.get(1, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == result, f"Results are not consistent. Expected {result}, got {r.json()} (Comparison on value)"

        r = c4.get(2, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == result, f"Results are not consistent. Expected {result}, got {r.json()} (Comparison on value)"

        # return score/reason
        return True, "ok"


def convergence_happens_before_basic_1(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1, 2]))
        fx.broadcast_view(conductor.get_shard_view())
        c1 = KVSMultiClient(fx.clients, "c1", log)

        log("\n> TEST CONVERGENCE HAPPENS BEFORE BASIC 1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # Put x = 0 on 0
        r = c1.put(0, "x", "0")
        assert r.ok, f"expected successful response for new key, got {r.status_code}"

        # Put x = 1 on 1
        r = c1.put(1, "x", "1")
        assert r.ok, f"expected successful response, got {r.status_code}"

        # Put x = 2 on 2
        r = c1.put(2, "x", "2")
        assert r.ok, f"expected successful response, got {r.status_code}"

        r = c1.get(0, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        result = r.json()["value"]

        r = c1.get(1, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == result, f"Results are not consistent. Expected {result}, got {r.json()} (Comparison on value)"

        r = c1.get(2, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == result, f"Results are not consistent. Expected {result}, got {r.json()} (Comparison on value)"

        # return score/reason
        return True, "ok"

# basic
def convergence_get_all_1(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())
        alice = KVSMultiClient(fx.clients, "alice", log)
        bob = KVSMultiClient(fx.clients, "bob", log)

        log("\n> TEST CONVERGENCE GETALL 1")

        # starts off with no partitions
        conductor.my_partition([0], "part1")
        conductor.my_partition([1], "part2")
        # conductor.my_partition([0,1], "part1")
        log("Created initial partitions: {0}, {1}")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        r = alice.put(0, "x", "alice")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = alice.put(1, "z", "testing123")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = bob.put(1, "x", "bob")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = bob.put(1, "y", "builder")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        
        # should hang here
        r = bob.get_all(0)
        assert r.status_code == 408, (
            f"expected 408 for get_all, got {r.status_code}"
        )
        
        # heal partition
        conductor.my_partition([0, 1], "base")
        log("Partition healed: {0, 1}")
        time.sleep(10)
        # sleep to allow for convergence

        r = bob.get_all(0)
        assert r.ok, f"expected ok for get_all, got {r.status_code}"
        KVS_state_0 = r.json()["items"]
        
        r = bob.get_all(1)
        assert r.ok, f"expected ok for get_all, got {r.status_code}"
        KVS_state_1 = r.json()["items"]

        assert KVS_state_0 == KVS_state_1, (
            f"KVS states are not equal to Bob: {KVS_state_0}, {KVS_state_1}"
        )

        r = alice.get_all(1)
        assert r.ok, f"expected ok for get_all, got {r.status_code}"
        KVS_state_2 = r.json()["items"]

        r = alice.get_all(0)
        assert r.ok, f"expected ok for get_all, got {r.status_code}"
        KVS_state_3 = r.json()["items"]

        assert KVS_state_0 == KVS_state_1 == KVS_state_2 == KVS_state_3, (
            f"KVS states are not equal to Alice: {KVS_state_0}, {KVS_state_1}, {KVS_state_2}, {KVS_state_3}"
        )

        return True, "ok"

# advanced
def convergence_get_all_2(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1, 2]))
        fx.broadcast_view(conductor.get_shard_view())
        alice = KVSMultiClient(fx.clients, "alice", log)
        bob = KVSMultiClient(fx.clients, "bob", log)
        carol = KVSMultiClient(fx.clients, "carol", log)

        c1 = KVSMultiClient(fx.clients, "c1", log)
        c2 = KVSMultiClient(fx.clients, "c2", log)
        c3 = KVSMultiClient(fx.clients, "c3", log)

        log("\n> TEST CONVERGENCE GET ALL 2")

        # partition 0, 1, 2
        conductor.create_partition([0], "p0")
        conductor.create_partition([1], "p1")
        conductor.create_partition([2], "p2")

        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # alice writes x, y, z to server 0
        r = alice.put(0, "x", "alice")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        r = alice.put(0, "y", "alice")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        r = alice.put(0, "z", "alice")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # bob writes x,y,z to server 1
        r = bob.put(1, "x", "bob")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        r = bob.put(1, "y", "bob")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        r = bob.put(1, "z", "bob")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # carol writes x, y, z to server 2
        r = carol.put(2, "x", "carol")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        r = carol.put(2, "y", "carol")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        r = carol.put(2, "z", "carol")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = c1.get_all(0)
        assert r.status_code == 200, f"expected 200 for get_all, got {r.status_code}"
        assert r.json()["items"] == {"x": "alice", "y": "alice", "z": "alice"}, (
            f"expected {{'x': 'alice', 'y': 'alice', 'z': 'alice'}}, got {r.json()}"
        )

        r = c2.get_all(1)
        assert r.status_code == 200, f"expected 200 for get_all, got {r.status_code}"
        assert r.json()["items"] == {"x": "bob", "y": "bob", "z": "bob"}, (
            f"expected {{'x': 'bob', 'y': 'bob', 'z': 'bob'}}, got {r.json()}"
        )

        r = c3.get_all(2)
        assert r.status_code == 200, f"expected 200 for get_all, got {r.status_code}"
        assert r.json()["items"] == {"x": "carol", "y": "carol", "z": "carol"}, (
            f"expected {{'x': 'carol', 'y': 'carol', 'z': 'carol'}}, got {r.json()}"
        )

        conductor.create_partition([0, 1, 2], "base")
        log("created new partition and waiting for sync...")
        time.sleep(10)

        r1 = c1.get_all(0)
        assert r.status_code == 200, f"expected 200 for get_all, got {r.status_code}"
        r2 = c2.get_all(1)
        assert r.status_code == 200, f"expected 200 for get_all, got {r.status_code}"
        r3 = c3.get_all(2)
        assert r.status_code == 200, f"expected 200 for get_all, got {r.status_code}"
        assert (
            r1.json()["items"] == r2.json()["items"] == r3.json()["items"]
        ), f"expected all responses to be equal, got \n{r1.json()['items']}, \n{r2.json()['items']}, \n{r3.json()['items']}"
        return True, "ok"


CONVERGENCE_TESTS = [
    TestCase("convergence_concurrent_basic_1", convergence_concurrent_basic_1),
    TestCase("convergence_happens_before_basic_1", convergence_happens_before_basic_1),
    TestCase("convergence_get_all_1", convergence_get_all_1),
    TestCase("convergence_get_all_2", convergence_get_all_2),
]

