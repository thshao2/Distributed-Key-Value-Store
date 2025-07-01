#!/usr/bin/env python3

import os
import json
import requests
import subprocess
import time
import sys
from typing import List, Dict, Any, Optional

from .containers import ContainerBuilder, ClusterConductor, CONTAINER_ENGINE
from .testcase import TestCase
from .util import log

# TODO: for parallel test runs, use generated group id
CONTAINER_IMAGE_ID = "kvstore-hw2-test"
TEST_GROUP_ID = "hw2"


class TestRunner:
    def __init__(self, project_dir: str):
        self.project_dir = project_dir
        # builder to build container image
        self.builder = ContainerBuilder(
            project_dir=project_dir, image_id=CONTAINER_IMAGE_ID
        )
        # network manager to mess with container networking
        self.conductor = ClusterConductor(
            group_id=TEST_GROUP_ID,
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


"""
TEST SET: this list the test cases to run
add more tests by appending to this list
"""

from .tests.hello import hello_cluster
from .tests.basic import BASIC_TESTS
from .tests.fuzz import FUZZ_TESTS
from .tests.view_change import VIEW_TESTS
from .tests.extra_endpoints import ENDPOINT_TESTS
from .tests.multiple_partitions import MULTIPLE_PARTITION_TESTS

TEST_SET = []
TEST_SET.append(TestCase("hello_cluster", hello_cluster))
TEST_SET.extend(BASIC_TESTS)
TEST_SET.extend(FUZZ_TESTS)
TEST_SET.extend(VIEW_TESTS)
TEST_SET.extend(ENDPOINT_TESTS)
TEST_SET.extend(MULTIPLE_PARTITION_TESTS)

# set to True to stop at the first failing test
FAIL_FAST = True

if __name__ == "__main__":
    # project dir will be the cwd
    project_dir = os.getcwd()
    runner = TestRunner(project_dir=project_dir)
    # prepare to run tests
    runner.prepare_environment()

    # if an argument is provided, use it to filter tests
    if len(sys.argv) > 1:
        test_filter = sys.argv[1]
        log(f"filtering tests by: {test_filter}")
        TEST_SET = [t for t in TEST_SET if test_filter in t.name]

    # run tests
    log("\n== RUNNING TESTS ==")
    run_tests = []
    for test in TEST_SET:
        log(f"\n== TEST: [{test.name}] ==\n")
        run_tests.append(test)
        score, reason = test.execute(runner.conductor)

        log(f"\n")
        if score:
            log(f"✓ PASSED {test.name}")
        else:
            log(f"✗ FAILED {test.name}: {reason}")
            if FAIL_FAST:
                log("FAIL FAST enabled, stopping at first failure")
                break

    # summarize the status of all tests
    log("\n== TEST SUMMARY ==")
    for test in run_tests:
        log(f"  - {test.name}: {'✓' if test.score else '✗'}")

    # clean up
    runner.cleanup_environment()
