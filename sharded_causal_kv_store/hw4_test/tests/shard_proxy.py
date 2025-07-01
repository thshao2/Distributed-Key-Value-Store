from ..containers import ClusterConductor
from ..util import log, Logger
from ..kvs_api import KVSClient
from ..testcase import TestCase
from .helper import KVSTestFixture, KVSMultiClient

DEFAULT_TIMEOUT = 10

def basic_proxy_one_client(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=4) as fx:
        c = KVSMultiClient(fx.clients, "client", log)
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        conductor.add_shard("shard2", conductor.get_nodes([2, 3]))
        fx.broadcast_view(conductor.get_shard_view())
        # test 1
        # put 50 keys (at least one proxy expected here)
        # get_all() on one shard
        # then ask the other shard for that key (proxy MUST happen here)

        node_to_put = 0
        base_key = "key"
        for i in range(0, 300):
            r = c.put(node_to_put, f"{base_key}{i}", f"{i}", timeout=10)
            assert r.ok, f"expected ok for new key, got {r.status_code}"
            node_to_put += 1
            node_to_put = node_to_put % 4

        r = c.get_all(0, timeout=20)  # should get all of shard 1's keys
        assert r.ok, f"expected ok for get, got {r.status_code}"
        items = r.json()["items"]
        keys = items.keys()
        for key in keys:
            r = c.get(2, key, timeout=30)
            assert r.ok, f"expected ok for get, got {r.status_code}"
            assert r.json()["value"] == items[key], f"wrong value returned: {r.json()}"

        return True, "ok"


def basic_proxy_many_clients(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=7) as fx:
        c = KVSMultiClient(fx.clients, "client", log)
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        conductor.add_shard("shard2", conductor.get_nodes([2, 3]))
        conductor.add_shard("shard3", conductor.get_nodes([4, 5, 6]))
        fx.broadcast_view(conductor.get_shard_view())
        # test 1
        # put 50 keys (at least one proxy expected here)
        # get_all() on one shard
        # then ask the other shard for that key (proxy MUST happen here)

        node_to_put = 0
        base_key = "key"
        for i in range(0, 300):
            c1 = KVSMultiClient(fx.clients, "client", log)
            r = c1.put(node_to_put, f"{base_key}{i}", f"{i}", timeout=10)
            assert r.ok, f"expected ok for new key, got {r.status_code}"
            node_to_put += 1
            node_to_put = node_to_put % 7

        r = c.get_all(0, timeout=20)  # should get all of shard 1's keys
        assert r.ok, f"expected ok for get, got {r.status_code}"
        items = r.json()["items"]
        keys = items.keys()
        for key in keys:
            r = c.get(2, key, timeout=30)
            assert r.ok, f"expected ok for get, got {r.status_code}"
            assert r.json()["value"] == items[key], f"wrong value returned: {r.json()}"

        return True, "ok"


def basic_proxy_partitioned_shards(
    conductor: ClusterConductor, dir, log: Logger, timeout=5 * DEFAULT_TIMEOUT
):
    with KVSTestFixture(conductor, dir, log, node_count=4) as fx:
        c = KVSMultiClient(fx.clients, "client", log)
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        conductor.add_shard("shard2", conductor.get_nodes([2, 3]))
        fx.broadcast_view(conductor.get_shard_view())
        conductor.create_partition([2, 3], "secondshard")

        helper(c, timeout=timeout)
        return True, "ok"


def helper(c: KVSMultiClient, timeout=5 * DEFAULT_TIMEOUT):
    ###
    # test 2
    # partition the shards
    # put a bunch of keys
    # we MUST probablistically encounter some hanging there.
    # have a time out where if it doesnt hang after like 50 keys, then its just wrong.
    node_to_put = 0
    base_key = "key"
    for i in range(0, 50):
        r = c.put(node_to_put, f"{base_key}{i}", f"{i}", timeout=10)
        assert r.ok or r.status_code == 408, (
            f"expected ok for new key, got {r.status_code}"
        )
        node_to_put += 1
        node_to_put = node_to_put % 4


PROXY_TESTS = [
    TestCase("basic_proxy_one_client", basic_proxy_one_client),
    TestCase("basic_proxy_many_clients", basic_proxy_many_clients),
    TestCase("basic_proxy_partitioned_shards", basic_proxy_partitioned_shards),
]
