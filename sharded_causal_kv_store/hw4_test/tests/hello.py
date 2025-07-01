from typing import List, Dict, Any, Optional
import requests

from ..containers import ClusterConductor
from ..util import log, Logger
from ..kvs_api import KVSClient


from typing import List, Dict, Any, Optional
import requests

def hello_cluster(conductor: ClusterConductor, dir, log: Logger):
    # create a cluster
    log("\n> SPAWN CLUSTER")
    conductor.spawn_cluster(node_count=2)

    # by default, all nodes are in the same partition, on the base network
    # let's create two partitions, one with node 0 and one with node 1
    log("\n> CREATE PARTITIONS")
    conductor.create_partition(node_ids=[0], partition_id="p0")
    conductor.create_partition(node_ids=[1], partition_id="p1")

    # describe cluster
    log("\n> DESCRIBE CLUSTER")
    conductor.describe_cluster()

    # talk to node 0 in the cluster
    log("\n> TALK TO NODE 0")
    n0_ep = conductor.node_external_endpoint(0)
    n0_client = KVSClient(n0_ep)
    n0_client.ping().raise_for_status()
    log(f"  - node 0 is up at {n0_ep}")

    # talk to node 1 in the cluster
    log("\n> TALK TO NODE 1")
    n1_ep = conductor.node_external_endpoint(1)
    n1_client = KVSClient(n1_ep)
    n1_client.ping().raise_for_status()
    log(f"  - node 1 is up at {n1_ep}")

    conductor.dump_all_container_logs(dir)
    # clean up
    log("\n> DESTROY CLUSTER")
    conductor.destroy_cluster()

    # return score/reason
    return True, "ok"
