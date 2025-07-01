"""Credits: Alex's testing repo"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import requests

from ..containers import ClusterConductor
from ..util import log, Logger
from ..kvs_api import KVSClient

DEFAULT_TIMEOUT = 5

import asyncio

class KVSTestFixture:
    def __init__(self, conductor: ClusterConductor, dir, log: Logger, node_count: int):
        conductor._parent = self
        self.conductor = conductor
        self.dir = dir
        self.node_count = node_count
        self.clients: list[KVSClient] = []
        self.log = log

    def spawn_cluster(self):
        self.log("\n> SPAWN CLUSTER")
        self.conductor.spawn_cluster(node_count=self.node_count)

        for i in range(self.node_count):
            ep = self.conductor.node_external_endpoint(i)
            self.clients.append(KVSClient(ep))

            r = self.clients[i].ping()
            assert r.status_code == 200, f"expected 200 for ping, got {r.status_code}"
            self.log(f"  - node {i} is up: {r.text}")

    def broadcast_view(self, view: List[Dict[str, Any]]):
        self.log(f"\n> SEND VIEW: {view}")
        for i, client in enumerate(self.clients):
            r = client.send_view(view)
            assert r.status_code == 200, (
                f"expected 200 to ack view, got {r.status_code}"
            )
            self.log(f"view sent to node {i}: {r.status_code} {r.text}")

    async def parallel_broadcast_view(self, view: Dict[str, List[Dict[str, Any]]]):
        self.log(f"\n> SEND VIEW: {view}")

        async def send_view(client: KVSClient, i: int):
            r = await client.async_send_view(view)
            assert r.status == 200, f"expected 200 to ack view, got {r.status}"
            self.log(f"view sent to node {i}: {r.status} {r.text}")

        tasks = [send_view(client, i) for i, client in enumerate(self.clients)]
        await asyncio.gather(*tasks)

    def rebroadcast_view(self, new_view: Dict[str, List[Dict[str, Any]]]):
        for i, client in enumerate(self.clients):
            r = client.resend_last_view_with_ips_from_new_view(new_view)
            if r is None:
                return
            assert r.status_code == 200, (
                f"expected 200 to ack view, got {r.status_code}"
            )
            self.log(f"view resent to node {i}: {r.status_code} {r.text}")

    def send_view(self, node_id: int, view: Dict[str, List[Dict[str, Any]]]):
        r = self.clients[node_id].send_view(view)
        assert r.status_code == 200, f"expected 200 to ack view, got {r.status_code}"
        self.log(f"view sent to node {node_id}: {r.status_code} {r.text}")

    def destroy_cluster(self):
        self.conductor.dump_all_container_logs(self.dir)
        self.log("\n> DESTROY CLUSTER")
        self.conductor.destroy_cluster()

    def __enter__(self):
        self.spawn_cluster()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.destroy_cluster()


class KVSMultiClient:
    def __init__(
        self, clients: List[KVSClient], name: str, log: Logger, persist_metadata=True
    ):
        self.clients = clients
        self._metadata = None
        self.name = name
        self.req = 0
        self.log = log
        self.persist_metadata = persist_metadata

        # internal model of kvs
        self._kvs_model = {}

    @property
    def metadata(self):
        """I'm the 'x' property."""
        print("getter of x called")
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        print("setter of x called")
        self._metadata = value if self.persist_metadata else None

    def reset_model(self):
        self._kvs_model = {}
        self.metadata = None

    def put(self, node_id: int, key: str, value: str, timeout: float = DEFAULT_TIMEOUT):
        self.log(
            f" {self.name} req_id:{self.req} > {node_id} > kvs.put {key} <- {value}"
        )

        r = self.clients[node_id].put(key, value, self.metadata, timeout=timeout)

        # update model if successful
        if r.status_code // 100 == 2:
            self._kvs_model[key] = value
            self.log(f" {self.name} req_id:{self.req} {r.json()}")
            self.metadata = r.json()["causal-metadata"]

        self.req += 1
        return r

    def get(self, node_id: int, key: str, timeout: float = DEFAULT_TIMEOUT):
        self.log(
            (f' {self.name} req_id:{self.req} > {node_id}> kvs.get '
            f'{key} request "causal-metadata": {self.metadata}')
        )
        r = self.clients[node_id].get(key, self.metadata, timeout=timeout)

        if r.status_code // 100 == 2:
            self.log(
                f" {self.name} req_id:{self.req} > {node_id}> kvs.get {key} -> {r.json()}"
            )
            self.metadata = r.json()["causal-metadata"]
        else:
            self.log(
                f" {self.name} req_id:{self.req} > {node_id}> kvs.get {key} -> HTTP ERROR {r.status_code}"
            )

        self.req += 1
        return r

    def get_all(self, node_id: int, timeout: float = DEFAULT_TIMEOUT):
        self.log(
            f' {self.name} req_id:{self.req} > {node_id} > kvs.get_all request "causal-metadata": {self.metadata}'
        )
        r = self.clients[node_id].get_all(self.metadata, timeout=timeout)
        if r.status_code // 100 == 2:
            self.log(
                f" {self.name} req_id:{self.req} > {node_id}> kvs.get_all -> {r.json()}"
            )
            self.metadata = r.json()["causal-metadata"]
        else:
            self.log(
                f" {self.name} req_id:{self.req} > {node_id} > kvs.get_all -> HTTP ERROR {r.status_code}"
            )

        self.req += 1
        return r
