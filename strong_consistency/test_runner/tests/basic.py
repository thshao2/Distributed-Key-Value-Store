from typing import List, Dict, Any, Optional
import requests

from ..containers import ClusterConductor
from ..util import log
from ..kvs_api import KVSClient
from ..testcase import TestCase

from .helper import KVSTestFixture, KVSMultiClient


def basic_kvs_put_get_1(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)

        # let's see if it behaves like a kvs
        log("\n> TEST KVS PUT/GET")

        # put a new kvp
        r = mc.put(0, "test1", "hello")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        # get should return same value
        r = mc.get(0, "test1")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == "hello", f"wrong value returned: {r.json()}"

        # we should be able to update it
        r = mc.put(0, "test1", "world")
        assert r.status_code == 200, f"expected 200 for update, got {r.status_code}"

        # verify update worked
        r = mc.get(0, "test1")
        assert r.json()["value"] == "world", "update failed"

        # return score/reason
        return True, "ok"


def basic_kvs_put_get_2(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)

        # let's see if it behaves like a kvs
        log("\n> TEST KVS PUT/GET")

        # put a new kvp
        r = mc.put(0, "test1", "hello")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        # now let's talk to the other node and see if it agrees
        r = mc.get(1, "test1")
        assert r.json()["value"] == "hello", "node 1 did not agree"

        # now let's try updating via node 1 and see if node 0 agrees
        r = mc.put(1, "test1", "bye")
        assert r.status_code == 200, f"expected 200 for update, got {r.status_code}"

        # see if node 0 agrees
        r = mc.get(0, "test1")
        assert r.json()["value"] == "bye", "node 0 did not agree"

        # return score/reason
        return True, "ok"


def basic_kvs_shrink_1(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=3) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)

        # put a new kvp
        log("\n> PUT NEW KEY")
        r = mc.put(0, "test1", "hello")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        # now let's isolate each node and make sure they agree
        log("\n> ISOLATE NODES")

        # isolate node 0
        conductor.create_partition([0], "p0")
        fx.send_view(0, conductor.get_partition_view("p0"))

        # isolate node 1
        conductor.create_partition([1], "p1")
        fx.send_view(1, conductor.get_partition_view("p1"))

        # isolate node 2
        conductor.create_partition([2], "p2")
        fx.send_view(2, conductor.get_partition_view("p2"))

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # make sure the data is there
        log("\n> MAKE SURE EACH ISOLATED NODE HAS THE DATA")

        r = mc.get(0, "test1")
        assert r.json()["value"] == "hello", "node 0 lost data"

        r = mc.get(1, "test1")
        assert r.json()["value"] == "hello", "node 1 lost data"

        r = mc.get(2, "test1")
        assert r.json()["value"] == "hello", "node 2 lost data"

        # return score/reason
        return True, "ok"


def basic_kvs_split_1(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=4) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)

        # put a new kvp
        log("\n> PUT NEW KEY")
        r = mc.put(0, "test1", "hello")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        # now let's partition 0,1 and 2,3
        log("\n> PARTITION NODES")

        # partition 0,1
        conductor.create_partition([0, 1], "p0")
        fx.send_view(0, conductor.get_partition_view("p0"))
        fx.send_view(1, conductor.get_partition_view("p0"))

        # partition 2,3
        conductor.create_partition([2, 3], "p1")
        fx.send_view(2, conductor.get_partition_view("p1"))
        fx.send_view(3, conductor.get_partition_view("p1"))

        # describe the new network topology
        log("\n> NETWORK TOPOLOGY")
        conductor.describe_cluster()

        # send different stuff to each partition
        log("\n> TALK TO PARTITION p0")
        r = mc.get(0, "test1")
        assert r.json()["value"] == "hello", "node 0 lost data"

        r = mc.put(1, "test2", "01")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        r = mc.get(0, "test2")
        assert r.json()["value"] == "01", "node 0 disagreed"

        log("\n> TALK TO PARTITION p1")
        r = mc.put(2, "test3", "23")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        r = mc.get(3, "test3")
        assert r.json()["value"] == "23", "node 3 disagreed"

        # return score/reason
        return True, "ok"


BASIC_TESTS = [
    TestCase("basic_kvs_put_get_1", basic_kvs_put_get_1),
    TestCase("basic_kvs_put_get_2", basic_kvs_put_get_2),
    TestCase("basic_kvs_shrink_1", basic_kvs_shrink_1),
    TestCase("basic_kvs_split_1", basic_kvs_split_1),
]
