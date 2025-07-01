"""Start up development environment with a certain amount of nodes.

`python devenv.py -n <number of clusters to run>`

If no -n specified - default value is 3
"""
import argparse
import os
import json
import requests
import subprocess
import time
import sys
from typing import List, Dict, Any, Optional

from hw2_test.containers import ContainerBuilder, ClusterConductor, CONTAINER_ENGINE
from hw2_test.testcase import TestCase
from hw2_test.util import log

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
    args = parser.parse_args()
    cluster_number = args.cluster_number
    print(f"Cluster number is {cluster_number}")

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
