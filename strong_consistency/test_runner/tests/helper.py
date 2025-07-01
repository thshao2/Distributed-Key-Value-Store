from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import requests

from ..containers import ClusterConductor
from ..util import log
from ..kvs_api import KVSClient


class KVSTestFixture:
    # conductor: ClusterConductor
    # node_count: int
    # clients: List[KVSClient]

    def __init__(self, conductor: ClusterConductor, node_count: int):
        self.conductor = conductor
        self.node_count = node_count
        self.clients = []

    def spawn_cluster(self):
        log("\n> SPAWN CLUSTER")
        self.conductor.spawn_cluster(node_count=self.node_count)

        for i in range(self.node_count):
            ep = self.conductor.node_external_endpoint(i)
            self.clients.append(KVSClient(ep))

            r = self.clients[i].ping()
            assert r.status_code == 200, f"expected 200 for ping, got {r.status_code}"
            log(f"  - node {i} is up: {r.text}")

    def broadcast_view(self, view: List[Dict[str, Any]]):
        log(f"\n> SEND VIEW: {view}")
        for i, client in enumerate(self.clients):
            r = client.send_view(view)
            assert (
                r.status_code == 200
            ), f"expected 200 to ack view, got {r.status_code}"
            log(f"view sent to node {i}: {r.status_code} {r.text}")

    def send_view(self, node_id: int, view: List[Dict[str, Any]]):
        r = self.clients[node_id].send_view(view)
        assert r.status_code == 200, f"expected 200 to ack view, got {r.status_code}"
        log(f"view sent to node {node_id}: {r.status_code} {r.text}")

    def destroy_cluster(self):
        log("\n> DESTROY CLUSTER")
        self.conductor.destroy_cluster()

    def __enter__(self):
        self.spawn_cluster()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.destroy_cluster()


class KVSMultiClient:
    def __init__(self, clients: List[KVSClient]):
        self.clients = clients

        # internal model of kvs
        self._kvs_model = {}

    def reset_model(self):
        self._kvs_model = {}

    def put(self, node_id: int, key: str, value: str):
        log(f"  {node_id}> kvs.put {key} <- {value}")
        r = self.clients[node_id].put(key, value)

        # update model if successful
        if r.status_code // 100 == 2:
            self._kvs_model[key] = value

        return r

    def get(self, node_id: int, key: str):
        r = self.clients[node_id].get(key)

        if r.status_code // 100 == 2:
            val = r.json()["value"]
            log(f"  {node_id}> kvs.get {key} -> {val}")
        else:
            log(f"  {node_id}> kvs.get {key} -> HTTP ERROR {r.status_code}")

        return r
    
    def delete(self, node_id: int, key: str):
        log(f"  {node_id}> kvs.delete {key}")
        r = self.clients[node_id].delete(key)

        if r.status_code // 100 == 2:
            if key in self._kvs_model:
                self._kvs_model.pop(key, None)
            log(f"  {node_id}> kvs.delete {key} -> {r.json()}")
        else:
            log(f"  {node_id}> kvs.delete {key} -> HTTP ERROR {r.status_code}")

        return r

    def get_all(self, node_id: int):
        log(f"  {node_id}> kvs.get_all")
        r = self.clients[node_id].get_all()

        if r.status_code // 100 == 2:
            log(f"  {node_id}> kvs.get_all -> {r.status_code} {r.json()}")
        else:
            log(f"  {node_id}> kvs.get_all -> HTTP ERROR {r.status_code}")

        return r
    
    def ping(self, node_id: int):
        r = self.clients[node_id].ping()

        if r.status_code != 200:
            log(f"  {node_id}> kvs.ping -> HTTP ERROR {r.status_code}")

        return r

