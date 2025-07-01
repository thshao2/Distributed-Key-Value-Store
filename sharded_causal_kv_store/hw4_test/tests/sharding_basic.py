from ..containers import ClusterConductor
from ..util import log, Logger
from ..kvs_api import KVSClient
from ..testcase import TestCase
from .helper import KVSTestFixture, KVSMultiClient


def basic_kv_view_accept(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=4) as fx:
        c = KVSMultiClient(fx.clients, "client", log)
        conductor.add_shard("shard1", conductor.get_nodes([0, 1, 2]))
        fx.broadcast_view(conductor.get_shard_view())

        r = c.put(0, "x", "1")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = c.put(1, "y", "2")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = c.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        r = c.get(1, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        r = c.get(0, "y")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "2", f"wrong value returned: {r.json()}"

        r = c.get(1, "y")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "2", f"wrong value returned: {r.json()}"

        return True, 0


def basic_kv_1(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=4) as fx:
        c = KVSMultiClient(fx.clients, "client", log)
        conductor.add_shard("shard1", conductor.get_nodes([0]))
        conductor.add_shard("shard2", conductor.get_nodes([1]))
        fx.broadcast_view(conductor.get_shard_view())

        r = c.put(0, "x", "1")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = c.put(1, "y", "2")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = c.get(0, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        r = c.get(1, "x")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "1", f"wrong value returned: {r.json()}"

        r = c.get(0, "y")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "2", f"wrong value returned: {r.json()}"

        r = c.get(1, "y")
        assert r.ok, f"expected ok for get, got {r.status_code}"
        assert r.json()["value"] == "2", f"wrong value returned: {r.json()}"

        return True, 0

def basic_kv_verify_proxy(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=4) as fx:
        c1 = KVSMultiClient(fx.clients, "c1", log)
        c2 = KVSMultiClient(fx.clients, "c2", log)
        c3 = KVSMultiClient(fx.clients, "c3", log)
        c4 = KVSMultiClient(fx.clients, "c4", log)
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        conductor.add_shard("shard2", conductor.get_nodes([2, 3]))
        fx.broadcast_view(conductor.get_shard_view())
        
        keys = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"]
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

        for i in range(len(keys)):
            key = keys[i]
            value = str(values[i])
            r = c1.put(0, key, value)
            assert r.ok, f"expected ok for new key, got {r.status_code}"

        conductor.create_partition([0,1], "p0")
        conductor.create_partition([2,3], "p1")

        r = c2.get_all(0)
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        shard1_keys = r.json()["items"] 

        r = c3.get_all(2)
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        shard2_keys = r.json()["items"] 

        print(shard1_keys)
        print(shard2_keys)
        assert ((len(shard1_keys) > 0) and (len(shard2_keys) > 0)), "One of the shards has no keys, this is extremely unlikely (1/2^11) and probably means something is wrong"

        rk1 = list(shard1_keys.keys())[0]
        rk2 = list(shard2_keys.keys())[0]

        r = c4.put(0, rk2, "This should fail")
        assert r.status_code == 408, f"expected 408 for new key, got {r.status_code}"

        r = c4.put(2, rk1, "This should also fail")
        assert r.status_code == 408, f"expected 408 for new key, got {r.status_code}"

        conductor.create_partition([0, 1, 2, 3], "base")

        r = c4.put(0, rk2, "This should work")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = c4.put(2, rk1, "This should also work")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

        r = c2.get_all(0)
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        shard1_keys = r.json()["items"]

        r = c3.get_all(2)
        assert r.ok, f"expected ok for new key, got {r.status_code}"
        shard2_keys = r.json()["items"]

        print(shard1_keys)
        print(shard2_keys)
        assert (len(shard1_keys) > 0 and len(shard2_keys) > 0), "One of the shards has no keys, this is extremely unlikely (1/2^11) and probably means something is wrong"

        return True, 0


BASIC_TESTS = [TestCase("basic_kv_1", basic_kv_1), TestCase("basic_kv_view_accept", basic_kv_view_accept), TestCase("basic_kv_verify_proxy", basic_kv_verify_proxy)]
