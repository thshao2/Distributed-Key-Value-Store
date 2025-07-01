from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import os
import json
import subprocess
import time
import re

from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
import requests

from .util import log, run_cmd_bg

CONTAINER_ENGINE = os.getenv("ENGINE", "docker")


class ContainerBuilder:
    def __init__(self, project_dir: str, image_id: str):
        self.project_dir = project_dir
        self.image_id = image_id

    def build_image(self) -> None:
        # ensure we are able to build the container image
        log(f"building container image {self.image_id}...")

        cmd = [CONTAINER_ENGINE, "build", "-t", self.image_id, self.project_dir]
        run_cmd_bg(cmd, verbose=True, error_prefix="failed to build container image")

        # ensure the image exists
        log(f"inspecting container image {self.image_id}...")
        cmd = [CONTAINER_ENGINE, "image", "inspect", self.image_id]
        run_cmd_bg(cmd, verbose=True, error_prefix="failed to inspect container image")


@dataclass
class ClusterNode:
    name: str  # container name
    index: int  # container global id/index
    ip: str  # container ip on current/primary network
    port: int  # container http service port
    external_port: (
        int  # host's mapped external port forwarded to container's service port
    )
    networks: List[str]  # networks the container is attached to

    def internal_endpoint(self) -> str:
        return f"http://{self.ip}:{self.port}"

    def external_endpoint(self) -> str:
        return f"http://localhost:{self.external_port}"


class ClusterConductor:
    def __init__(self, group_id: str, base_image: str, external_port_base: int = 8081):
        self.group_id = group_id
        self.base_image = base_image
        self.base_port = external_port_base
        self.nodes: List[ClusterNode] = []

        # naming patterns
        self.group_ctr_prefix = f"kvs_{group_id}_node"
        self.group_net_prefix = f"kvs_{group_id}_net"

        # base network
        self.base_net_name = f"{self.group_net_prefix}_base"

    def _list_containers(self) -> List[str]:
        # get list of all container names
        try:
            output = subprocess.check_output(
                [CONTAINER_ENGINE, "ps", "-a", "--format", "{{.Names}}"]
            )
            return output.decode().strip().split("\n")
        except subprocess.CalledProcessError as e:
            log("failed to list containers")
            log(e.stderr.decode())
            raise

    def _list_networks(self) -> List[str]:
        # get list of all network names
        try:
            output = subprocess.check_output(
                [CONTAINER_ENGINE, "network", "ls", "--format", "{{.Name}}"]
            )
            return output.decode().strip().split("\n")
        except subprocess.CalledProcessError as e:
            log("failed to list networks")
            log(e.stderr.decode())
            raise

    def _remove_container(self, name: str) -> None:
        # remove a single container
        log(f"removing container {name}")
        run_cmd_bg(
            [CONTAINER_ENGINE, "rm", "-f", name],
            verbose=True,
            error_prefix=f"failed to remove container {name}",
        )

    def _remove_network(self, name: str) -> None:
        # remove a single network
        log(f"removing network {name}")
        run_cmd_bg(
            [CONTAINER_ENGINE, "network", "rm", name],
            verbose=True,
            error_prefix=f"failed to remove network {name}",
            check=False,
        )

    def _create_network(self, name: str) -> None:
        # create a single network
        log(f"creating network {name}")
        run_cmd_bg(
            [CONTAINER_ENGINE, "network", "create", name],
            verbose=True,
            error_prefix=f"failed to create network {name}",
        )

    def _network_exists(self, name: str) -> bool:
        return name in self._list_networks()

    def cleanup_hanging(self, group_only: bool = True) -> None:
        # if group_only, only clean up stuff for this group
        # otherwise clean up anything kvs related
        if group_only:
            log(f"cleaning up group {self.group_id}")
            container_pattern = f"^kvs_{self.group_id}_.*"
            network_pattern = f"^kvs_{self.group_id}_net_.*"
        else:
            log("cleaning up all kvs containers and networks")
            container_pattern = "^kvs_.*"
            network_pattern = "^kvs_net_.*"

        # compile regex patterns
        container_regex = re.compile(container_pattern)
        network_regex = re.compile(network_pattern)

        # cleanup containers
        log(f"  cleaning up {'group' if group_only else 'all'} containers")
        containers = self._list_containers()
        for container in containers:
            if container and container_regex.match(container):
                self._remove_container(container)

        # cleanup networks
        log(f"  cleaning up {'group' if group_only else 'all'} networks")
        networks = self._list_networks()
        for network in networks:
            if network and network_regex.match(network):
                self._remove_network(network)

    # we can check if a node is online by GET /ping
    def _is_online(self, node: ClusterNode) -> bool:
        try:
            r = requests.get(f"{node.external_endpoint()}/ping")
            return r.status_code == 200
        except requests.exceptions.RequestException as e:
            log(f"node {node.name} is not online: {e}")
            return False

    def _node_name(self, index: int) -> str:
        return f"kvs_{self.group_id}_node_{index}"

    def node_external_endpoint(self, index: int) -> str:
        return self.nodes[index].external_endpoint()

    # create a cluster of nodes on the base network
    def spawn_cluster(self, node_count: int) -> None:
        log(f"spawning cluster of {node_count} nodes")

        # delete base network if it exists
        run_cmd_bg([CONTAINER_ENGINE, "network", "rm", self.base_net_name], check=False)

        # create base network
        run_cmd_bg(
            [CONTAINER_ENGINE, "network", "create", self.base_net_name],
            verbose=True,
            error_prefix="failed to create base network",
        )

        # spawn the nodes
        for i in range(node_count):
            node_name = self._node_name(i)
            # map to sequential external port
            external_port = self.base_port + i
            port = 8081  # internal port

            log(f"  starting container {node_name} (ext_port={external_port})")

            # start container detached from networks
            run_cmd_bg(
                [
                    CONTAINER_ENGINE,
                    "run",
                    "-d",
                    "--name",
                    node_name,
                    "--env",
                    f"NODE_IDENTIFIER={i}",
                    "-p",
                    f"{external_port}:{port}",
                    self.base_image,
                ],
                verbose=True,
                error_prefix=f"failed to start container {node_name}",
            )

            # attach container to base network
            log(f"    attaching container {node_name} to base network")
            run_cmd_bg(
                [CONTAINER_ENGINE, "network", "connect", self.base_net_name, node_name],
                verbose=True,
                error_prefix=f"failed to attach container {node_name} to base network",
            )

            # inspect the container to get ip, etc.
            log(f"    inspecting container {node_name}")
            try:
                inspect = subprocess.run(
                    [CONTAINER_ENGINE, "inspect", node_name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                log(f"failed to inspect container {node_name}")
                log(e.stderr.decode())
                raise
            info = json.loads(inspect.stdout)[0]

            container_ip = info["NetworkSettings"]["Networks"][self.base_net_name][
                "IPAddress"
            ]

            # store container metadata
            node = ClusterNode(
                name=node_name,
                index=i,
                ip=container_ip,
                port=port,
                external_port=external_port,
                networks=[self.base_net_name],
            )
            self.nodes.append(node)

            log(f"    container {node_name} spawned, base_net_ip={container_ip}")

        # wait for the nodes to come online (sequentially)
        log("waiting for nodes to come online...")
        wait_online_start = time.time()
        wait_online_timeout = 10
        for i in range(node_count):
            node = self.nodes[i]
            while not self._is_online(node):
                if time.time() - wait_online_start > 10:
                    raise RuntimeError(f"node {node.name} did not come online")
                time.sleep(0.2)

            log(f"  node {node.name} online")

        log("all nodes online")

    def destroy_cluster(self) -> None:
        # clean up after this group
        self.cleanup_hanging(group_only=True)

        # clear nodes
        self.nodes.clear()

    def describe_cluster(self) -> None:
        log(f"TOPOLOGY: group {self.group_id}")
        log(f"nodes:")
        for node in self.nodes:
            log(
                f"  {node.name}: {node.ip}:{node.port} <-> localhost:{node.external_port}"
            )

        # now log the partitions and the nodes they contain
        partitions = {}
        for node in self.nodes:
            for network in node.networks:
                if network not in partitions:
                    partitions[network] = []
                partitions[network].append(node.index)

        log(f"partitions:")
        for net, nodes in partitions.items():
            part_name = net[len(self.group_net_prefix) + 1 :]
            log(f"  {part_name}: {nodes}")

    def create_partition(self, node_ids: List[int], partition_id: str) -> None:
        net_name = f"kvs_{self.group_id}_net_{partition_id}"

        log(f"creating partition {partition_id} with nodes {node_ids}")

        # create partition network if it doesn't exist
        if not self._network_exists(net_name):
            self._create_network(net_name)

        # disconnect specified nodes from all other networks
        log(f"  disconnecting nodes from other networks")
        for i in node_ids:
            node = self.nodes[i]
            for network in node.networks:
                if network != net_name:
                    log(f"    disconnecting {node.name} from network {network}")
                    run_cmd_bg(
                        [CONTAINER_ENGINE, "network", "disconnect", network, node.name],
                        verbose=True,
                        error_prefix=f"failed to disconnect {node.name} from network {network}",
                    )
                    node.networks.remove(network)

        # connect nodes to partition network, and update node ip
        log(f"  connecting nodes to partition network {net_name}")
        for i in node_ids:
            node = self.nodes[i]
            log(f"    connecting {node.name} to network {net_name}")
            run_cmd_bg(
                [CONTAINER_ENGINE, "network", "connect", net_name, node.name],
                verbose=True,
                error_prefix=f"failed to connect {node.name} to network {net_name}",
            )
            node.networks.append(net_name)

            # update node ip on the new network
            inspect = subprocess.run(
                [CONTAINER_ENGINE, "inspect", node.name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            info = json.loads(inspect.stdout)[0]
            container_ip = info["NetworkSettings"]["Networks"][net_name]["IPAddress"]
            log(f"    node {node.name} ip in network {net_name}: {container_ip}")

            # update node ip
            node.ip = container_ip

    def get_full_view(self):
        view = []
        for node in self.nodes:
            view.append({"address": f"{node.ip}:{node.port}", "id": node.index})
        return view

    def get_partition_view(self, partition_id: str):
        net_name = f"kvs_{self.group_id}_net_{partition_id}"
        view = []
        for node in self.nodes:
            if net_name in node.networks:
                view.append({"address": f"{node.ip}:{node.port}", "id": node.index})
        return view

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # clean up automatically
        self.destroy_cluster()
