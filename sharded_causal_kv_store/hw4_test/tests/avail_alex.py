"""Credits: Alex's testing repo"""

from time import sleep

from ..containers import ClusterConductor
from ..testcase import TestCase
from ..util import log, Logger
from .helper import KVSMultiClient, KVSTestFixture
import time


def availability_basic_1(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())
        c1 = KVSMultiClient(fx.clients, "c1", log)
        c2 = KVSMultiClient(fx.clients, "c2", log)

        log("\n> TEST AVAILABILE BASIC 1")

        # partition 0,1
        conductor.create_partition([0], "p0")
        conductor.create_partition([1], "p1")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # Put x = 1 on 0 (paritioned from 1)
        r = c1.put(0, "x", "1")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # Get it back
        r = c1.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        # Get x from 1 (should be 404)
        r = c2.get(1, "x")
        assert r.status_code == 404, f"expected 404 for get, got {r.status_code}"

        # Create x = 2
        r = c2.put(1, "x", "2")
        assert r.ok, f"expected ok for update, got {r.status_code}"

        # return score/reason
        return True, "ok"

def availability_basic_2(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1, 2]))
        fx.broadcast_view(conductor.get_shard_view())
        c1 = KVSMultiClient(fx.clients, "c1", log)
        c2 = KVSMultiClient(fx.clients, "c2", log)
        c3 = KVSMultiClient(fx.clients, "c3", log)

        log("\n> TEST AVAILABLE BASIC 2")

        # partition 0,1, 2
        conductor.create_partition([0], "p0")
        conductor.create_partition([1], "p1")
        conductor.create_partition([2], "p2")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # Put x = 1 on 0 (paritioned from everything else)
        r = c1.put(0, "x", "1")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # Put x = 2 on 1 (paritioned from everything else)
        r = c2.put(1, "x", "2")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # Put x = 3 on 2 (paritioned from everything else)
        r = c3.put(2, "x", "3")
        assert r.ok, f"expected ok for new key, got {r.status_code}"


        # Get it back
        r = c1.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        # Get it back
        r = c2.get(1, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "2", f"wrong value returned: {r.json()}"

        # Get it back
        r = c3.get(2, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "3", f"wrong value returned: {r.json()}"

        # return score/reason
        return True, "ok"
    

def availability_basic_3(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1, 2]))
        fx.broadcast_view(conductor.get_shard_view())
        c1 = KVSMultiClient(fx.clients, "c1", log)
        c2 = KVSMultiClient(fx.clients, "c2", log)

        log("\n> TEST AVAILABLE BASIC 3")

        # partition 0,1, 2
        conductor.create_partition([0], "p0")
        conductor.create_partition([1], "p1")
        conductor.create_partition([2], "p2")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # Put x = 1 on 0 (paritioned from everything else)
        r = c2.put(0, "x", "1")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # Put x = 2 on 1 (paritioned from everything else)
        r = c2.put(1, "x", "2")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # Put x = 3 on 2 (paritioned from everything else)
        r = c2.put(2, "x", "3")
        assert r.ok, f"expected ok for new key, got {r.status_code}"


        # Get it back
        r = c1.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        # Get it back
        r = c1.get(1, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "2", f"wrong value returned: {r.json()}"

        # Get it back
        r = c1.get(2, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "3", f"wrong value returned: {r.json()}"

        # return score/reason
        return True, "ok"
    

def availability_advanced_1(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1, 2]))
        fx.broadcast_view(conductor.get_shard_view())
        c1 = KVSMultiClient(fx.clients, "c1", log)  # writer
        c2 = KVSMultiClient(fx.clients, "c2", log)
        c3 = KVSMultiClient(fx.clients, "c3", log)
        c4 = KVSMultiClient(fx.clients, "c4", log)

        log("\n> TEST AVAILABILE ADVANCED 1")

        # partition 0,1
        conductor.create_partition([0], "p0")
        conductor.create_partition([1], "p1")
        conductor.create_partition([2], "p2")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # Put x = 1 on 0 (isolated)
        r = c1.put(0, "x", "1")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # Put x = 1 on 1 (isolated)
        r = c1.put(1, "x", "2")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        # Put x = 1 on 2 (isolated)
        r = c1.put(2, "x", "3")
        assert r.ok, f"expected ok for new key, got {r.status_code}"


        # Get it back
        r = c2.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        # Get it back
        r = c3.get(1, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "2", f"wrong value returned: {r.json()}"

        # Get it back
        r = c4.get(2, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "3", f"wrong value returned: {r.json()}"

        # return score/reason
        return True, "ok"



AVAILABILITY_ALEX_TESTS = [
    TestCase("availability_basic_1", availability_basic_1),
    TestCase("availability_basic_2", availability_basic_2),
    TestCase("availability_basic_3", availability_basic_3),
    TestCase("availability_advanced_1", availability_advanced_1),
]
