"""Start up development environment with a certain amount of nodes in the same network.

`python devenv.py -n <number of clusters to run> -shards <"True" to create 1 shard per node, "False" for all nodes in 1 shard>`

Example: python devenv.py -shards False

If no -n specified - default value is 3
If no -shards specified - default value is True (1 shard per node)


Assignment 4 modifications:
- Shards are created in view
- Depending on the shards argument, we may create all nodes in 1 shard, or one shard per node
"""
import argparse
import os
import json
import requests
import subprocess
import time
import sys
from typing import List, Dict, Any, Optional

from hw4_test.containers import ContainerBuilder, ClusterConductor, CONTAINER_ENGINE
from hw4_test.testcase import TestCase
from hw4_test.util import log
from hw4_test.tests.helper import KVSTestFixture

# TODO: for parallel test runs, use generated group id
CONTAINER_IMAGE_ID = "kvstore-hw2-test-dev"
DEV_GROUP_ID = "hw2-dev"


class TestRunner:
    def __init__(self, project_dir: str):
        self.project_dir = project_dir
        # builder to build container image
        self.builder = ContainerBuilder(
            project_dir=project_dir, image_id=CONTAINER_IMAGE_ID
        )
        # network manager to mess with container networking
        self.conductor = ClusterConductor(
            group_id=DEV_GROUP_ID,
            base_image=CONTAINER_IMAGE_ID,
            external_port_base=9000,
        )

    def prepare_environment(self) -> None:
        log("\n-- prepare_environment --")
        # build the container image
        self.builder.build_image()

        # aggressively clean up anything kvs-related
        # NOTE: this disallows parallel run processes, so turn it off for that
        self.conductor.cleanup_hanging(group_only=False)

    def cleanup_environment(self) -> None:
        log("\n-- cleanup_environment --")
        # destroy the cluster
        self.conductor.destroy_cluster()
        # aggressively clean up anything kvs-related
        # NOTE: this disallows parallel run processes, so turn it off for that
        self.conductor.cleanup_hanging(group_only=True)



if __name__ == "__main__":
    # gather command line information
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--cluster_number", help="Number of Clusters", type=int, default=3)
    parser.add_argument("-shards", "--multiple_shards", help="Enable Shards (True/False) - True for 1 shard per node, False for 1 shard overall", type=lambda x: x.lower() == "true", default=True)
    args = parser.parse_args()
    cluster_number = args.cluster_number
    multiple_shards = args.multiple_shards
    print(f"Cluster number is {cluster_number}")
    print(f"Should make 1 shard per node: {multiple_shards}")

    # project dir will be the cwd
    project_dir = os.getcwd()
    runner = TestRunner(project_dir=project_dir)
    # prepare to run tests
    runner.prepare_environment()
    print("Finished preparing environment")
    # start up cluster
    conductor = ClusterConductor(DEV_GROUP_ID, CONTAINER_IMAGE_ID)
    conductor.spawn_cluster(cluster_number)
    print(conductor.nodes)
    # broadcast view to everyone
    if multiple_shards: # 1 Shard per node
        overall_view = conductor.get_full_view()
        view = {f"Shard{x}":[node] for (x, node) in enumerate(overall_view)}
    else: # All nodes in same shard
        view = {"Shard1": conductor.get_full_view()}
    print("view", view)
    for i in range(8081, 8081+cluster_number):
        r = requests.put(f"http://localhost:{i}/view", json={"view": view})
        assert r.status_code == 200
        print("Sent view to node", (i-8081))
        print(r.json())

