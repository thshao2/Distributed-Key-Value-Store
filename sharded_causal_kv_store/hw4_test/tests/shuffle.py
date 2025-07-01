from ..containers import ClusterConductor
from ..util import log, Logger
from ..kvs_api import KVSClient
from ..testcase import TestCase
from .helper import KVSTestFixture, KVSMultiClient

import time

DEFAULT_TIMEOUT = 10

def basic_shuffle_add_remove(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        c = KVSMultiClient(fx.clients, "client", log, persist_metadata=False)
        conductor.add_shard("shard1", conductor.get_nodes([0]))
        conductor.add_shard("shard2", conductor.get_nodes([1]))

        fx.broadcast_view(conductor.get_shard_view())

        node_to_put = 0
        base_key = "key"
        # Put 15 keys
        for i in range(15):
            log(f"Putting key {i}\n")
            c.reset_model()
            r = c.put(node_to_put, f"{base_key}{i}", f"{i}", timeout=10)
            assert r.ok, f"expected ok for new key, got {r.status_code}"
            node_to_put += 1
            node_to_put = node_to_put % 2
        c.reset_model()

        # Get all keys
        r = c.get_all(0, timeout=10)
        c.reset_model()
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys = res

        c.reset_model()

        r = c.get_all(1, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard2_keys = res

        c.reset_model()

        log(f"Shard 1 keys: {shard1_keys}\n")
        log(f"Shard 2 keys: {shard2_keys}\n")

        # Total number of keys should matched number of keys put
        assert len(shard1_keys) + len(shard2_keys) == 15, (
            f"expected 15 keys, got {len(shard1_keys) + len(shard2_keys)}"
        )

        # Add a 3rd shard, causing a shuffle. There should still be 15 keys at the end.
        log("Adding 3rd shard\n")
        conductor.add_shard("shard3", conductor.get_nodes([2]))
        fx.broadcast_view(conductor.get_shard_view())

        # Get the keys on shard 1
        r = c.get_all(0, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys = res
        c.reset_model()

        log(f"Shard 1 keys: {shard1_keys}\n")

        # get the keys on shard 2
        r = c.get_all(1, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard2_keys = res
        c.reset_model()

        log(f"Shard 2 keys: {shard2_keys}\n")

        # get the keys on shard 3
        r = c.get_all(2, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard3_keys = res

        c.reset_model()

        log(f"Shard 3 keys: {shard3_keys}\n")

        assert len(shard1_keys) + len(shard2_keys) + len(shard3_keys) == 15, (
            f"expected 15 keys, got {len(shard1_keys) + len(shard2_keys) + len(shard3_keys)}"
        )

        log("Remove Shard 3\n")
        # Remove shard 3, causing a shuffle. Move Node 2 to shard 1 so the keys should still exist, and be shuffled
        conductor.remove_shard("shard3")
        fx.broadcast_view(conductor.get_shard_view())

        r = c.get_all(0, timeout=10)  # should get all of shard 1's keys
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys = res

        c.reset_model()

        r = c.get_all(1, timeout=10)  # should get all of shard 2's keys
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard2_keys = res

        assert len(shard1_keys) + len(shard2_keys) == 15, (
            f"expected 15 keys, got {len(shard1_keys) + len(shard2_keys)}"
        )

        log("Remove Shard 2\n")
        # Remove shard 2. This loses keys.
        conductor.remove_shard("shard2")
        fx.broadcast_view(conductor.get_shard_view())

        r = c.get_all(0, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys_after_delete = res

        c.reset_model()

        assert len(shard1_keys_after_delete) == 15, (
            f"expected 15 keys, got {len(shard1_keys_after_delete)}"
        )

        return True, "ok"


def basic_shuffle_1(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        c = KVSMultiClient(fx.clients, "client", log, persist_metadata=False)
        conductor.add_shard("shard1", conductor.get_nodes([0]))
        conductor.add_shard("shard2", conductor.get_nodes([1]))

        fx.broadcast_view(conductor.get_shard_view())

        node_to_put = 0
        base_key = "key"
        # Put 15 keys
        for i in range(15):
            log(f"Putting key {i}\n")
            r = c.put(node_to_put, f"{base_key}{i}", f"{i}", timeout=10)
            assert r.ok, f"expected ok for new key, got {r.status_code}"
            node_to_put += 1
            node_to_put = node_to_put % 2

        # Get all keys
        r = c.get_all(0, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys = res

        r = c.get_all(1, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard2_keys = res

        log(f"Shard 1 keys: {shard1_keys}\n")
        log(f"Shard 2 keys: {shard2_keys}\n")

        # Total number of keys should matched number of keys put
        assert len(shard1_keys) + len(shard2_keys) == 15, (
            f"expected 15 keys, got {len(shard1_keys) + len(shard2_keys)}"
        )

        # Add a 3rd shard, causing a shuffle. There should still be 15 keys at the end.
        log("Adding 3rd shard\n")
        conductor.add_shard("shard3", conductor.get_nodes([2]))
        fx.broadcast_view(conductor.get_shard_view())

        # Get the keys on shard 1
        r = c.get_all(0, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys = res

        log(f"Shard 1 keys: {shard1_keys}\n")

        # get the keys on shard 2
        r = c.get_all(1, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard2_keys = res

        log(f"Shard 2 keys: {shard2_keys}\n")

        # get the keys on shard 3
        r = c.get_all(2, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard3_keys = res

        log(f"Shard 3 keys: {shard3_keys}\n")

        assert len(shard1_keys) + len(shard2_keys) + len(shard3_keys) == 15, (
            f"expected 15 keys, got {len(shard1_keys) + len(shard2_keys) + len(shard3_keys)}"
        )

        # Remove shard 3, causing a shuffle. Move Node 2 to shard 1 so the keys should still exist, and be shuffled
        conductor.remove_shard("shard3")
        conductor.add_node_to_shard("shard1", conductor.get_node(2))
        fx.broadcast_view(conductor.get_shard_view())

        r = c.get_all(0, timeout=10)  # should get all of shard 1's keys
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys = res

        r = c.get_all(1, timeout=10)  # should get all of shard 2's keys
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard2_keys = res

        assert len(shard1_keys) + len(shard2_keys) == 15, (
            f"expected 15 keys, got {len(shard1_keys) + len(shard2_keys)}"
        )

        # Remove shard 2. This loses keys.
        conductor.remove_shard("shard2")
        conductor.remove_node_from_shard("shard1", conductor.get_node(2))
        fx.broadcast_view(conductor.get_shard_view())

        r = c.get_all(0, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys_after_delete = res

        assert len(shard1_keys_after_delete) == 15, (
            f"expected 15 keys, got {len(shard1_keys_after_delete)}"
        )

        return True, "ok"


def basic_shuffle_2(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        c = KVSMultiClient(fx.clients, "client", log)
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())

        ## basic shuffle 2
        # view= 1 shard with 2 nodes
        # put 50 keys
        # get_all keys from shard 1
        # add shard with 1 node
        # get_all keys from shard 2
        # get_all keys from shard 1
        # check both returned sets are disjoint and that their union makes the original get_all results

        node_to_put = 0
        base_key = "key"
        for i in range(0, 300):
            r = c.put(node_to_put, f"{base_key}{i}", f"{i}", timeout=10)
            assert r.ok, f"expected ok for new key, got {r.status_code}"
            node_to_put += 1
            node_to_put = node_to_put % 2

        r = c.get_all(0, timeout=30)  # should get all of shard 1's keys
        assert r.ok, f"expected ok for get, got {r.status_code}"
        original_get_all = r.json()["items"]

        assert len(original_get_all.keys()) == 300, (
            f"original_get_all doesn't have 300 keys, instead has {len(original_get_all.keys())} keys"
        )

        conductor.add_shard("shard2", conductor.get_nodes([2]))
        fx.broadcast_view(conductor.get_shard_view())

        r = c.get_all(2, timeout=30)  # should get all of shard 2's keys
        assert r.ok, f"expected ok for get, got {r.status_code}"
        get_all_1 = r.json()["items"]
        keys1 = set(get_all_1.keys())

        r = c.get_all(1, timeout=30)  # should get all of shard 1's keys
        assert r.ok, f"expected ok for get, got {r.status_code}"
        get_all_2 = r.json()["items"]
        keys2 = set(get_all_2.keys())

        for key in keys1:
            assert key not in keys2, "key not in keys2"
        for key in keys2:
            assert key not in keys1, "key not in keys2"

        assert original_get_all.keys() == keys1.union(keys2), (
            f"get all keys does not equal key1 joined with keys2. diff one way: \n{keys1.union(keys2).difference(original_get_all.keys())}\n diff other way:\n{set(original_get_all.keys()).difference(keys1.union(keys2))}"
        )
        assert len(original_get_all) == len(keys1) + len(keys2), "lengths do not match"

        return True, "ok"


def basic_shuffle_3(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        c = KVSMultiClient(fx.clients, "client", log, persist_metadata=False)
        conductor.add_shard("shard1", conductor.get_nodes([0]))
        conductor.add_shard("shard2", conductor.get_nodes([1]))

        fx.broadcast_view(conductor.get_shard_view())

        node_to_put = 0
        base_key = "key"
        # Put 15 keys
        for i in range(15):
            log(f"Putting key {i}\n")
            r = c.put(node_to_put, f"{base_key}{i}", f"{i}", timeout=10)
            assert r.ok, f"expected ok for new key, got {r.status_code}"
            node_to_put += 1
            node_to_put = node_to_put % 2

        # Get all keys
        r = c.get_all(0, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys = res

        r = c.get_all(1, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard2_keys = res

        log(f"Shard 1 keys: {shard1_keys}\n")
        log(f"Shard 2 keys: {shard2_keys}\n")

        # Total number of keys should matched number of keys put
        assert len(shard1_keys) + len(shard2_keys) == 15, (
            f"expected 15 keys, got {len(shard1_keys) + len(shard2_keys)}"
        )

        # Add a 3rd shard, causing a shuffle. There should still be 15 keys at the end.
        log("Adding 3rd shard\n")
        conductor.add_shard("shard3", conductor.get_nodes([2]))
        fx.broadcast_view(conductor.get_shard_view())

        # Get the keys on shard 1
        r = c.get_all(0, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys = res

        log(f"Shard 1 keys: {shard1_keys}\n")

        # get the keys on shard 2
        r = c.get_all(1, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard2_keys = res

        log(f"Shard 2 keys: {shard2_keys}\n")

        # get the keys on shard 3
        r = c.get_all(2, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard3_keys = res

        log(f"Shard 3 keys: {shard3_keys}\n")

        assert len(shard1_keys) + len(shard2_keys) + len(shard3_keys) == 15, (
            f"expected 15 keys, got {len(shard1_keys) + len(shard2_keys) + len(shard3_keys)}"
        )

        # Remove shard 3, causing a shuffle. Move Node 2 to shard 1 so the keys should still exist, and be shuffled
        conductor.remove_shard("shard3")
        conductor.add_node_to_shard("shard1", conductor.get_node(2))
        fx.broadcast_view(conductor.get_shard_view())

        r = c.get_all(0, timeout=10)  # should get all of shard 1's keys
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys = res

        r = c.get_all(1, timeout=10)  # should get all of shard 2's keys
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard2_keys = res

        assert len(shard1_keys) + len(shard2_keys) == 15, (
            f"expected 15 keys, got {len(shard1_keys) + len(shard2_keys)}"
        )

        # Remove shard 2. This loses keys.
        conductor.remove_shard("shard2")
        fx.broadcast_view(conductor.get_shard_view())

        r = c.get_all(0, timeout=10)
        assert r.ok, f"expected ok for get, got {r.status_code}"
        res = r.json()["items"]
        shard1_keys_after_delete = res

        assert len(shard1_keys_after_delete) == 15, (
            f"expected 15 keys, got {len(shard1_keys_after_delete)}"
        )

        return True, "ok"


SHUFFLE_TESTS = [
    TestCase("basic_shuffle_add_remove", basic_shuffle_add_remove),
    TestCase("basic_shuffle_1", basic_shuffle_1),
    TestCase("basic_shuffle_2", basic_shuffle_2),
    TestCase("basic_shuffle_3", basic_shuffle_3),
]
