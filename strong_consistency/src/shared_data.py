"""Static variables shared across all blueprints & app on a single node.

To access from any other file:
    1. from shared_data import SharedData
    2. SharedData.kvstore
"""

import os
from threading import Lock

class SharedData:
    kvstore = {}
    kvs_lock = Lock() # Mutex lock for kvstore

    NODE_IDENTIFIER = os.environ.get("NODE_IDENTIFIER", "0")

    # CURRENT VIEW
    #   A list of dicts: [ {"address": "172.4.0.2:8081", "id": 1}, ... ]
    current_view = []

    # ROLE: "primary" or "backup"
    role = None # default, updated after /view is set