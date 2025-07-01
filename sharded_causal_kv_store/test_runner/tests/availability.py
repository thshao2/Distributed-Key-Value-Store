from typing import List, Dict, Any, Optional
import requests

from ..containers import ClusterConductor
from ..util import log
from ..kvs_api import KVSClient
from ..testcase import TestCase

from .helper import KVSTestFixture, KVSMultiClient
import time

def availability_basic_1(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        c1 = KVSMultiClient(fx.clients, "c1")
        c2 = KVSMultiClient(fx.clients, "c2")

        log("\n> TEST AVAILABILE BASIC 1")

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
        r = c1.get(0, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        # Get x from 1 (should be 404)
        r = c2.get(1, "x")
        assert r.status_code == 404, f"expected 404 for get, got {r.status_code}"

        # Create x = 2
        r = c2.put(1, "x", "2")
        assert r.status_code == 200, f"expected 200 for update, got {r.status_code}"

        # return score/reason
        return True, "ok"


def availability_advanced_1(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=3) as fx:
        fx.broadcast_view(conductor.get_full_view())
        c1 = KVSMultiClient(fx.clients, "c1")  # writer
        c2 = KVSMultiClient(fx.clients, "c2")
        c3 = KVSMultiClient(fx.clients, "c3")
        c4 = KVSMultiClient(fx.clients, "c4")

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
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Put x = 1 on 1 (isolated)
        r = c1.put(1, "x", "2")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Put x = 1 on 2 (isolated)
        r = c1.put(2, "x", "3")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"


        # Get it back
        r = c2.get(0, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        # Get it back
        r = c3.get(1, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == "2", f"wrong value returned: {r.json()}"

        # Get it back
        r = c4.get(2, "x")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == "3", f"wrong value returned: {r.json()}"



        # return score/reason
        return True, "ok"

def availability_getall(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=3) as fx:
        fx.broadcast_view(conductor.get_full_view())
        c1 = KVSMultiClient(fx.clients, "c1")  # writer
        c2 = KVSMultiClient(fx.clients, "c2")
        c3 = KVSMultiClient(fx.clients, "c3")
        c4 = KVSMultiClient(fx.clients, "c4")

        log("\n> TEST AVAILABLE /data enpoint")

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # Put x = 1
        r = c1.put(0, "x", "1")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Put y = 2
        r = c1.put(1, "y", "2")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Put z = 3
        r = c1.put(2, "z", "3")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # wait for eventual consistency
        time.sleep(10)

        # partition nodes
        conductor.create_partition([0], "p0")
        conductor.create_partition([1], "p1")
        conductor.create_partition([2], "p2")

        # Get it back
        r = c2.get_all(0)
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        items = r.json()["items"]
        assert items["x"] == "1", f'expected x=1, got {items["x"]}'
        assert items["y"] == "2", f'expected y=2, got {items["y"]}'
        assert items["z"] == "3", f'expected z=3, got {items["z"]}'

        # Get it back
        r = c3.get_all(1)
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        items = r.json()["items"]
        assert items["x"] == "1", f'expected x=1, got {items["x"]}'
        assert items["y"] == "2", f'expected y=2, got {items["y"]}'
        assert items["z"] == "3", f'expected z=3, got {items["z"]}'

        # Get it back
        r = c4.get_all(2)
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        items = r.json()["items"]
        assert items["x"] == "1", f'expected x=1, got {items["x"]}'
        assert items["y"] == "2", f'expected y=2, got {items["y"]}'
        assert items["z"] == "3", f'expected z=3, got {items["z"]}'


        # return score/reason
        return True, "ok"

AVAILABILITY_TESTS = [
    TestCase("availability_basic_1", availability_basic_1),
    TestCase("availability_advanced_1", availability_advanced_1),
    TestCase("availability_getall", availability_getall)
]
