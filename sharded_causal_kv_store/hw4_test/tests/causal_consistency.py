from typing import List, Dict, Any, Optional
import requests

from ..containers import ClusterConductor
from ..util import log
from ..kvs_api import KVSClient
from ..testcase import TestCase

from .helper import KVSTestFixture, KVSMultiClient
import time
import threading

def test_causal_basic(conductor: ClusterConductor):
  """If PUT(x=1) -> PUT(x=2), return x=2."""
  with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients, name="mc")

        # Put a value to node 0
        r = mc.put(0, "test1", "hello")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Put a different value to node 1
        r = mc.put(1, "test1", "world")

        # Wait for 10 second to allow the nodes to reach consistency
        time.sleep(8)

        # Get value from both requests and ensure eventual consistency
        r0 = mc.get(0, "test1")
        assert r0.status_code == 200, f"expect 200 for get to node 0, got {r0.status_code}"
        r1 = mc.get(1, "test1")
        assert r1.status_code == 200, f"expect 200 for get to node 1, got {r1.status_code}"
        assert r0.json()["value"] == "world"
        assert r1.json()["value"] == "world"

        # return score/reason
        return True, "ok"


def test_causal_view_expansion(conductor: ClusterConductor):
    """After a view expansion, the system is still causally consistent."""
    with KVSTestFixture(conductor, node_count=3) as fx:
        # Only include 2 of the nodes in the original view.
        fx.broadcast_view(conductor.get_full_view()[:-1])
        mc = KVSMultiClient(fx.clients, name="mc")

        # Put a value to node 1
        r = mc.put(1, "test1", "hello")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"
        
        # Perform a view expansion
        fx.broadcast_view(conductor.get_full_view())
        
        # Immediately try to get from the new view
        r = mc.get(2, "test1")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == "hello"

        # Ensure the view expansion didn't cause internal errors - do a few more operations
        r = mc.put(2, "test1", "2")
        assert r.status_code == 200, f"expected 200 for put to node 2, got {r.status_code}"
        r = mc.put(0, "test1", "0")
        assert r.status_code == 200, f"expected 200 for put to node 0, got {r.status_code}"
        r = mc.get(1, "test1")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.json()["value"] == "0", f"Most up to date value is 0, received {r.json()['value']} instead"

        return True, "ok"

def test_causal_hang(conductor: ClusterConductor):
    """Tests to see if system hangs when faced with a potential causal violation.
    
    Scenario 3 from https://docs.google.com/document/d/1lURYmjofk38QUBZ97IweqWVIEF8diPHu1RywjfgUPQs/edit?tab=t.0

    Setup:
      1. Client sends PUT(x=1) to N0.
      2. Partition happens between N0 and N1.
      3. Client sends PUT(x=2) to N1.
      4. Client sends PUT(y=1) to N0.
      5. Client reads x from N0 --> must hang.

    Explanation: 
      since PUT(x=1) -> PUT(x=2) -> PUT(y=1), the read for x must return x=2 or a newer value. However, doe to 
      the partition, N0 cannot access the newer value of x=2 since it's partitioned from N1. Thus, the only other 
      choice is to hang.
    """
    with KVSTestFixture(conductor, node_count=2) as fx:
        fx.broadcast_view(conductor.get_full_view())
        mc = KVSMultiClient(fx.clients, name="mc")

        # Step 1: Put 'x':1 to node 0
        r = mc.put(0, "x", "1")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"
        
        # Step 2: Partition node 1 so that node 0 is isolated from updates on node 1
        conductor.create_partition([1], "p1")

        # Step 3: Put 'x':2 to node 1
        r = mc.put(1, "x", "2")
        assert r.status_code == 200, f"expected 200 for updating key, got {r.status_code}"

        # Step 4: Put 'y':1 to node 0 to trigger the causal check on GET for 'x'
        r = mc.put(0, "y", "1")
        assert r.status_code == 200, f"expected 200 for new key, got {r.status_code}"

        # Dictionary to capture the result or exception from the GET request.
        result = {}

        def hanged_get():
            try:
                r = mc.get(0, "x", timeout=None)
                result["status_code"] = r.status_code
                result["value"] = r.json().get("value")
            except Exception as e:
                result["exception"] = str(e)

        # Step 5: Spawn the GET call in a separate thread and expect it to hang.
        import time
        start_time = time.monotonic()
        get_thread = threading.Thread(target=hanged_get)
        get_thread.start()
        # Wait up to 10 seconds
        get_thread.join(timeout=10)
        elapsed = time.monotonic() - start_time

        # Assert that the thread is still alive (i.e., the GET is hanging)
        assert get_thread.is_alive(), f"Get request returned in {elapsed:.2f} seconds, expected it to hang"

        # Now, remove the partition to allow the system to resolve the causal violation.
        conductor.create_partition([1], "base")
        # Wait again for the thread to finish (up to another 10 seconds)
        start_time = time.monotonic()
        get_thread = threading.Thread(target=hanged_get)
        get_thread.start()
        # Wait up to 10 seconds
        get_thread.join(timeout=10)
        elapsed = time.monotonic() - start_time

        assert not get_thread.is_alive(), f"Get request did not complete after partition removal in {elapsed:.2f} seconds"
        
        # Verify the GET call returned the expected result.
        assert result.get("status_code") == 200, f"Expected 200 after healing partition, got {result.get('status_code')}"
        assert result.get("value") == "2", f"Expected value '2', got {result.get('value')}"

        return True, "ok"


        
CAUSAL_CONSISTENCY_TESTS = [
  TestCase("test_causal_basic", test_causal_basic),
  TestCase("test_causal_view_expansion", test_causal_view_expansion),
  TestCase("test_causal_hang", test_causal_hang),
]