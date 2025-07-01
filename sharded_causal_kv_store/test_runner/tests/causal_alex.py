"""Credits: Alex's testing repo"""

from time import sleep

from ..containers import ClusterConductor
from ..testcase import TestCase
from ..util import log, Logger
from .helper import KVSMultiClient, KVSTestFixture
import time

def causal_basic_bob_smells(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())
        alice = KVSMultiClient(fx.clients, "alice", log)
        bob = KVSMultiClient(fx.clients, "bob", log)
        carol = KVSMultiClient(fx.clients, "carol", log)

        log("\n> TEST CAUSAL BASIC 1")

        # partition 0,1, 2
        conductor.create_partition([0], "p0")
        conductor.create_partition([1], "p1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # alice writes x = bs to server node 0
        r = alice.put(0, "x", "bob smells")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # bob reads x from server node 0
        r = bob.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob smells", f"wrong value returned: {r.json()}"

        # bob writes y = fua for server node 1
        r = bob.put(1, "y", "f you alice")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # carol reads y from server node 1
        r = carol.get(1, "y")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "f you alice", f"wrong value returned: {r.json()}"

        log("Reading x from server 1 (should hang until timeout expires)")
        # carol reads x from server node 1 - should hang!
        r = carol.get(1, "x", timeout=10)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get due to partition, got {r.status_code}"
        )

        # return score/reason
        return True, "ok"


def causal_heal_bob_smells(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())
        alice = KVSMultiClient(fx.clients, "alice", log)
        bob = KVSMultiClient(fx.clients, "bob", log)
        carol = KVSMultiClient(fx.clients, "carol", log)

        log("\n> TEST CAUSAL BASIC 2")

        # partition 0,1, 2
        conductor.my_partition([0], "base")
        conductor.my_partition([1], "p1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # alice writes x = bs to server node 0
        r = alice.put(0, "x", "bob smells")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # bob reads x from server node 0
        r = bob.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob smells", f"wrong value returned: {r.json()}"

        # bob writes y = fua for server node 1
        r = bob.put(1, "y", "f you alice")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # carol reads y from server node 1
        r = carol.get(1, "y")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "f you alice", f"wrong value returned: {r.json()}"

        log("Reading x from server 1 (should hang until timeout expires)")
        # carol reads x from server node 1 - should hang!
        r = carol.get(1, "x", timeout=10)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get due to partition, got {r.status_code}"
        )

        conductor.my_partition([0, 1], "base")
        log("created new parititon and waiting for sync...")
        sleep(10)

        r = carol.get(1, "x", timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob smells", f"wrong value returned: {r.json()}"

        # return score/reason
        return True, "ok"


def causal_bob_doesnt_smell_old(conductor: ClusterConductor, dir, log):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())
        alice = KVSMultiClient(fx.clients, "alice", log)
        bob = KVSMultiClient(fx.clients, "bob", log)
        carol = KVSMultiClient(fx.clients, "carol", log)

        log("\n> TEST CAUSAL BASIC OLD")

        # partition 0,1
        conductor.my_partition([0], "p0")
        conductor.my_partition([1], "p1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        r = alice.put(0, "x", "bob smells")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        log("Reading x from server 1 (should hang until timeout expires)")
        r = alice.get(1, "x", timeout=10)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get due to partition, got {r.status_code}"
        )

        firstValueWins = False

        r = bob.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob smells", f"wrong value returned: {r.json()}"
        log("Reading x from server 1 (should hang until timeout expires)")
        r = bob.get(1, "x", timeout=10)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get due to partition, got {r.status_code}"
        )
        r = bob.put(1, "y", "f you alice")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = carol.put(1, "x", "bob doesn't smell")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        r = bob.get(1, "x", timeout=10)
        if (r.ok):
            firstValueWins = True
            assert r.json()["value"] == "bob doesn't smell", (
                f"wrong value returned: {r.json()}"
            )
        else:
            firstValueWins = False
        
        r = bob.put(0, "y", "thanks carol")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        log(
            "Reading x from server 0 after writing x to server 1 (should hang until timeout expires)"
        )
        r = carol.get(0, "x", timeout=10)
        if (firstValueWins):
            assert r.status_code == 408, (
                f"expected 408 (timeout) for get due to partition, got {r.status_code}"
            )
        else:
            assert r.ok, f"expected ok for carol's final GET x request before partition heal"

        conductor.create_partition([0, 1], "base")
        log("created new parititon and waiting for sync...")
        sleep(10)

        r = alice.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob doesn't smell" if firstValueWins else "bob smells", (
            f"wrong value returned: {r.json()}"
        )
        r = carol.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob doesn't smell" if firstValueWins else "bob smells", (
            f"wrong value returned: {r.json()}"
        )
        log("Reading y from server 0(should NOT hang)")
        r = carol.get(1, "y")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "thanks carol", f"wrong value returned: {r.json()}"
        return True, "ok"



def causal_bob_doesnt_smell_updated(conductor, dir, log):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())
        alice = KVSMultiClient(fx.clients, "alice", log)
        bob = KVSMultiClient(fx.clients, "bob", log)
        carol = KVSMultiClient(fx.clients, "carol", log)

        log("\n> TEST CAUSAL BASIC 2")

        # partition 0,1
        conductor.my_partition([0], "base")
        conductor.my_partition([1], "p1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        r = alice.put(0, "x", "bob smells")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        log("Reading x from server 1 (should hang until timeout expires)")
        r = alice.get(1, "x", timeout=10)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get due to partition, got {r.status_code}"
        )

        r = bob.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob smells", f"wrong value returned: {r.json()}"
        log("Reading x from server 1 (should hang until timeout expires)")
        r = bob.get(1, "x", timeout=10)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get due to partition, got {r.status_code}"
        )
        r = bob.put(1, "y", "f you alice")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = carol.get(1, "y")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        r = carol.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        r = carol.put(1, "x", "bob doesn't smell")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        r = bob.get(1, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob doesn't smell", (
            f"wrong value returned: {r.json()}"
        )
        r = bob.put(0, "y", "thanks carol")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        log(
            "Reading x from server 0 after writing x to server 1 (should hang until timeout expires)"
        )
        r = carol.get(0, "x", timeout=10)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get due to partition, got {r.status_code}"
        )

        conductor.my_partition([0, 1], "base")
        log("created new parititon and waiting for sync...")
        sleep(10)

        r = alice.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob doesn't smell", (
            f"wrong value returned: {r.json()}"
        )
        r = carol.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob doesn't smell", (
            f"wrong value returned: {r.json()}"
        )
        log("Reading y from server 0(should NOT hang)")
        r = carol.get(1, "y")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "thanks carol", f"wrong value returned: {r.json()}"
        return True, "ok"


def causal_get_all_1(conductor, dir, log):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())
        conductor.my_partition([0], "p0")
        conductor.my_partition([1], "p1")
        alice = KVSMultiClient(fx.clients, "alice", log)
        bob = KVSMultiClient(fx.clients, "bob", log)
        carol = KVSMultiClient(fx.clients, "carol", log)

        r = alice.put(0, "x", "bob smells")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        r = alice.put(1, "y", "bob still smells! stinky :( ")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        log("Reading all from server 1 (should hang until timeout expires)")
        r = alice.get_all(0)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get_all due to partition, got {r.status_code}"
        )
        log("Reading all from server 1 (should hang until timeout expires)")
        r = alice.get_all(1)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get_all due to partition, got {r.status_code}"
        )

        r = bob.get_all(0)
        assert r.ok, f"expected ok for get_all, got {r.status_code}"
        assert r.json()["items"] == {"x": "bob smells"}, (
            f"wrong value returned: {r.json()}"
        )
        log("Reading all from server 1 (should hang until timeout expires)")
        r = bob.get_all(1, timeout=10)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get_all due to partition, got {r.status_code}"
        )
        r = carol.get_all(1, timeout=10)
        assert r.ok, f"expected ok for get_all, got {r.status_code}"
        assert r.json()["items"] == {"y": "bob still smells! stinky :( "}, (
            f"wrong value returned: {r.json()}"
        )
    return True, "ok"


def causal_bob_smells_read(conductor, dir, log):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())
        alice = KVSMultiClient(fx.clients, "alice", log)
        bob = KVSMultiClient(fx.clients, "bob", log)
        carol = KVSMultiClient(fx.clients, "carol", log)

        log("\n> TEST CAUSAL PUT NO UPDATE")

        # partition 0,1, 2
        conductor.create_partition([0], "p0")
        conductor.create_partition([1], "p1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # alice writes x = bs to server node 0
        r = alice.put(0, "x", "bob smells")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # bob reads x from server node 0
        r = bob.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob smells", f"wrong value returned: {r.json()}"

        # bob writes y = fua for server node 1
        r = bob.put(1, "y", "f you alice")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # carol reads y from server node 1
        r = carol.get(1, "x")
        assert r.status_code == 404, f"expected 404 for get, got {r.status_code}"

        log("Reading x from server 1 (should hang until timeout expires)")
        r = carol.get(1, "y", timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "f you alice", f"wrong value returned: {r.json()}"

        # carol reads x from server node 1 - should hang!
        r = carol.get(1, "x", timeout=10)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get due to partition, got {r.status_code}"
        )

        # return score/reason
        return True, "ok"


def causal_bob_smells_read_2(conductor, dir, log):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())
        alice = KVSMultiClient(fx.clients, "alice", log)
        bob = KVSMultiClient(fx.clients, "bob", log)
        carol = KVSMultiClient(fx.clients, "carol", log)

        log("\n> TEST CAUSAL PUT NO UPDATE")

        # partition 0,1, 2
        conductor.create_partition([0], "p0")
        conductor.create_partition([1], "p1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # alice initializes x = bss to server 1
        r = alice.put(1, "x", "bob still smells")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        # alice writes x = bs to server node 0
        r = alice.put(0, "x", "bob smells")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # bob reads x from server node 0
        r = bob.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "bob smells", f"wrong value returned: {r.json()}"

        # bob writes y = fua for server node 1
        r = bob.put(1, "y", "f you alice")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # carol reads y from server node 1
        r = carol.get(1, "x")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        assert r.json()["value"] == "bob still smells", (
            f"expected value differs, expected 'bob still smells', got {r.json()['value']}"
        )

        r = carol.get(1, "y", timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "f you alice", f"wrong value returned: {r.json()}"

        log("Reading x from server 1 (should hang until timeout expires)")
        # carol reads x from server node 1 - should hang!
        r = carol.get(1, "x", timeout=10)
        assert r.status_code == 408, (
            f"expected 408 (timeout) for get due to partition, got {r.status_code}"
        )

        # return score/reason
        return True, "ok"
    
def causal_basic_tiebreak(conductor, dir, log):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())
        alice = KVSMultiClient(fx.clients, "alice", log)
        bob = KVSMultiClient(fx.clients, "bob", log)
        carol = KVSMultiClient(fx.clients, "carol", log)
        david = KVSMultiClient(fx.clients, "david", log)

        log("\n> TEST CAUSAL BASIC tiebreak")

        # partition 0,1, 2
        conductor.create_partition([0], "p0")
        conductor.create_partition([1], "p1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        r = alice.put(0, "x", "1")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        
        r = bob.put(1, "x", "2")
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        
        r = alice.get(1, "x")
        alice_hang = r.status_code == 408
        r = bob.get(0, "x")
        bob_hang = r.status_code == 408
        
        conductor.create_partition([0, 1], "base")
        sleep(10)
        
        
        a = carol.get(0, "x")
        assert a.ok, f"expected ok for get, got {a.status_code}"
        b = david.get(1, "x")
        assert b.ok, f"expected ok for get, got {b.status_code}"
        
        assert a.json()["value"] == b.json()["value"], f"expected {a.json()['value']} == {b.json()['value']}"
        
        if a.json()["value"] == "1":
            assert alice_hang, f"expected alice to hang"
            assert not bob_hang, f"expected bob to not hang"
        if a.json()["value"] == "2":
            assert not alice_hang, f"expected alice to not hang"
            assert bob_hang, f"expected bob to hang"
            
        return True, "ok"
        
        

def causal_no_overwrite_future(conductor: ClusterConductor, dir: str, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())
        a = 0
        b = 1
        c = KVSMultiClient(fx.clients, "c", log)
        conductor.my_partition([a, b], "base")

        log("\n> TEST CAUSAL NO OVERWRITE FUTURE")

        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        assert c.put(a, "y", "old").ok, "expected ok for new key"
        assert c.put(a, "x", "1").ok, "expected ok for new key"

        log("waiting for sync before partitioning")
        sleep(10)

        assert c.get(b, "y").ok, "expected ok for get"
        assert c.get(b, "x").ok, "expected ok for get"

        conductor.my_partition([a], "a")
        conductor.my_partition([b], "b")
        log("Partitioned a and b")
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        assert c.put(a, "y", "new").ok, "expected ok for updated key"

        r = c.get(b, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        r = c.get(b, "y")
        assert r.status_code == 408, (
            f"expected timeout due to missing dependency {r.status_code}"
        )

        return True, "ok"


CAUSAL_ALEX_TESTS = [
    TestCase("causal_basic_bob_smells", causal_basic_bob_smells),
    TestCase("causal_heal_bob_smells", causal_heal_bob_smells),
    TestCase("causal_bob_doesnt_smell_old", causal_bob_doesnt_smell_old),
    TestCase("causal_bob_doesnt_smell_updated", causal_bob_doesnt_smell_updated),
    TestCase("causal_basic_tiebreak", causal_basic_tiebreak),
    TestCase("causal_get_all_1", causal_get_all_1),
    TestCase("causal_bob_smells_read", causal_bob_smells_read),
    TestCase("causal_bob_smells_read_2", causal_bob_smells_read_2),
    TestCase("causal_no_overwrite_future", causal_no_overwrite_future),
]
