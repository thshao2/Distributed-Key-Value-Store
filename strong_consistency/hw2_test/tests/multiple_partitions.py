# Credits: Cheuk on Discord in asgn2 channel

from typing import List, Dict, Any, Optional
import requests

from ..containers import ClusterConductor
from ..util import log
from ..kvs_api import KVSClient
from ..testcase import TestCase

from .helper import KVSTestFixture, KVSMultiClient

import requests
import threading
import time
import random
import string
import json

def kvs_multiple_partition(conductor: ClusterConductor):
    with KVSTestFixture(conductor, node_count=6) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients)
        testkeys = ["testkey1", "testkey2", "testkey3", "testkey4", "testkey5"]
        values = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]
        conductor.create_partition([0,1,2], "set1")
        conductor.create_partition([3,4,5], "set2")
        for i in range(3):
            fx.send_view(i, conductor.get_partition_view("set1"))
            fx.send_view(i+3, conductor.get_partition_view("set2"))
        p1mostRecent = {}
        p2mostRecent = {}
        for i in range(100):
            log(f"iteration {i}")
            key1 = random.choice(testkeys)
            values1 = random.choice(values)
            key2 = random.choice(testkeys)
            values2 = random.choice(values)
            match random.randint(0,2):
                case 0:
                    r = mc.put(random.randint(0,2), key1, values1)
                    # assert r.status_code in (200,201), f"expected 200 or 201 but got {r.status_code}"
                    if key1 in p1mostRecent:
                        assert r.status_code == 200, f"expected 200 but got {r.status_code}"
                    else:
                        assert r.status_code == 201, f"expected 201 but got {r.status_code}"
                    p1mostRecent[key1] = values1
                    r = mc.get(random.randint(3,5), key2)
                    if key2 in p2mostRecent:
                        assert r.json()["value"] == p2mostRecent[key2], f"expected {p2mostRecent[key2]} but got {r.json()['value']}"
                    else:
                        assert r.status_code == 404, f"expected 404 but got {r.status_code}"
                case 1:
                    r = mc.delete(random.randint(0,2), key1)
                    if key1 in p1mostRecent:
                        assert r.status_code == 200, f"expected 200 but got {r.status_code}"
                    else:
                        assert r.status_code == 404, f"expected 404 but got {r.status_code}"
                    p1mostRecent.pop(key1, None)
                    r = mc.put(random.randint(3,5), key2, values2)
                    if key2 in p2mostRecent:
                        assert r.status_code == 200, f"expected 200 but got {r.status_code}"
                    else:
                        assert r.status_code == 201, f"expected 201 but got {r.status_code}"
                    p2mostRecent[key2] = values2
                case 2:
                    r = mc.get(random.randint(0,2), key1)
                    if key1 in p1mostRecent:
                        assert r.json()["value"] == p1mostRecent[key1], f"expected {p1mostRecent[key1]} but got {r.json()['value']}"
                    else:
                        assert r.status_code == 404, f"expected 404 but got {r.status_code}"
                    r = mc.delete(random.randint(3,5), key2)
                    if key2 in p2mostRecent:
                        assert r.status_code == 200, f"expected 200 but got {r.status_code}"
                    else:
                        assert r.status_code == 404, f"expected 404 but got {r.status_code}"
                    p2mostRecent.pop(key2, None)
        for i in range(6):
            conductor.create_partition([i], f"p{i}")
            fx.send_view(i, conductor.get_partition_view(f"p{i}"))
            r = mc.get_all(i)
            assert r.status_code == 200, f"expected 200 but got {r.status_code}"
            expected = p1mostRecent if i < 3 else p2mostRecent
            assert r.json() == expected, f"expected {expected} but got {r.json()}"
        return True, "ok"


MULTIPLE_PARTITION_TESTS = [
  TestCase("kvs_multiple_partition", kvs_multiple_partition),
]
