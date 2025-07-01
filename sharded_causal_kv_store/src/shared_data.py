"""Static variables shared across all blueprints & app on a single node.

To access from any other file:
    1. from shared_data import SharedData
    2. SharedData.kvstore
"""

from packages.vector_clock import VectorClock
from packages.hash import HashCircle

import os
from asyncio import Lock

class SharedData:
    kvstore = {}
    lock = Lock() # Mutex lock used when accessing causal_metadata & kvstore

    NODE_IDENTIFIER = int(os.environ.get("NODE_IDENTIFIER", 0))

    # CURRENT VIEW
    #   Dict of list of dicts: {<ShardName>: [ {"address": "172.4.0.2:8081", "id": 1}, ... ]}
    current_view = {}

    # Nodes in the same shard to gossip to (randomized for more effective/faster gossip)
    gossip_nodes = []

    # The current shard this node is in
    current_shard = None

    # list of all shards in the system
    shards = []

    # Hash Circle
    hash_circle = HashCircle()

    causal_data = {}