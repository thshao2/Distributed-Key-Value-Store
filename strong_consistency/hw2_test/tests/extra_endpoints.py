from typing import List, Dict, Any, Optional
import requests

from ..containers import ClusterConductor
from ..util import log
from ..kvs_api import KVSClient
from ..testcase import TestCase

from .helper import KVSTestFixture, KVSMultiClient


def kvs_delete(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)

        # let's see if it behaves like a kvs
        log("\n> TEST KVS DELETE")

        # put values into kvs
        r = mc.put(0, "test1", "hello")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        r = mc.put(0, "test2", "hello2")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        #delete a key
        r = mc.delete(0, "test1")
        assert r.status_code == 200, f"expected 200 for correct delete, got {r.status_code}"

        r = mc.get(0, "test1")
        assert r.status_code == 404, f"expected 404 for correct read, got {r.status_code}"

        #proxy delete a key
        r = mc.delete(1, "test2")
        assert r.status_code == 200, f"expected 200 for correct delete, got {r.status_code}"

        r = mc.get(0, "test2")
        assert r.status_code == 404, f"expected 404 for correct read, got {r.status_code}"

        #delete a key that does not exist
        r = mc.delete(0, "doesntexist")
        assert r.status_code == 404, f"expected 404 for correct delete, got {r.status_code}"

        return True, "ok"

def kvs_get_all(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)

        # let's see if it behaves like a kvs
        log("\n> TEST KVS DATA")

        #get all on empty kvs
        r = mc.get_all(0)
        assert r.status_code == 200, f"expected 200 for get_all, got {r.status_code}"
        assert len(r.json()) == 0, f"expected empty get_all"

        # put values into kvs
        r = mc.put(0, "test1", "hello")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        r = mc.put(0, "test2", "hello2")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        r = mc.put(0, "test3", "hello3")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        # get_all on primary
        r = mc.get_all(0)
        assert r.status_code == 200, f"expected 200 for get_all, got {r.status_code}"
        values = r.json()
        assert values['test1'] == 'hello', f"wrong value in get_all"
        assert values['test2'] == 'hello2', f"wrong value in get_all"
        assert values['test3'] == 'hello3', f"wrong value in get_all"

        return True, "ok"

def kvs_ping(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)

        # let's see if it behaves like a kvs
        log("\n> TEST KVS PING")

        r = mc.ping(0)
        assert r.status_code == 200, f"expected 200, got {r.status_code}"

        r = mc.ping(1)
        assert r.status_code == 200, f"expected 200, got {r.status_code}"

        return True, "ok"

def test_kvs_delete(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)

        # let's see if it behaves like a kvs
        log("\n> TEST KVS PUT/DEL")

        # put a new kvp
        r = mc.put(0, "test1", "hello")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        # we should be able to update it
        r = mc.put(0, "test1", "world")
        assert r.status_code == 200, f"expected 200 for update, got {r.status_code}"

        r = mc.delete(0, "test1")
        assert r.status_code == 200, f"expected 200 for delete, got {r.status_code}"

        log("Checking key is removed")
        r = mc.get(0, "test1")
        assert r.status_code == 404, f"expected 404 for delete, got {r.status_code}"

        # return score/reason
        return True, "ok"

def test_kvs_get_all(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)

        # let's see if it behaves like a kvs
        log("\n> TEST KVS PUT/GET_ALL")

        r = mc.put(0, "test1", "hello")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

        r = mc.put(0, "test2", "world")
        assert r.status_code == 201, f"expected 200 for update, got {r.status_code}"

        r = mc.put(0, "test3", "how")
        assert r.status_code == 201, f"expected 200 for update, got {r.status_code}"

        r = mc.put(0, "test4", "are")
        assert r.status_code == 201, f"expected 200 for update, got {r.status_code}"

        r = mc.put(0, "test5", "you")
        assert r.status_code == 201, f"expected 200 for update, got {r.status_code}"

        r = mc.get_all(0)
        assert r.status_code == 200, f"expected 200 for delete, got {r.status_code}"

        # return score/reason
        return True, "ok"


ENDPOINT_TESTS = [
    TestCase("kvs_delete", kvs_delete),
    TestCase("kvs_get_all", kvs_get_all),
    TestCase("kvs_ping", kvs_ping),
    TestCase("kvs_delete_2", test_kvs_delete),
    TestCase("kvs_get_all_2", test_kvs_get_all),
]
