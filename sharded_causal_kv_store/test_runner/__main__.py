#!/usr/bin/env python3

import os
import json
import requests
import subprocess
import time
import re
import datetime
from multiprocessing.pool import ThreadPool
import sys
from typing import List, Dict, Any, Optional

from .containers import ContainerBuilder, ClusterConductor, CONTAINER_ENGINE
import argparse
from .testcase import TestCase
from .util import log, global_logger, Logger

# TODO: for parallel test runs, use generated group id
CONTAINER_IMAGE_ID = "kvstore-hw4-test"
TEST_GROUP_ID = "hw4"

debug = False

class TestRunner:
    def __init__(self, project_dir: str, debug_output_dir: str):
        self.project_dir = project_dir
        self.debug_output_dir = debug_output_dir
        # builder to build container image
        self.builder = ContainerBuilder(
            project_dir=project_dir, image_id=CONTAINER_IMAGE_ID
        )
        # network manager to mess with container networking
        self.conductor = ClusterConductor(
            group_id=TEST_GROUP_ID,
            base_image=CONTAINER_IMAGE_ID,
            external_port_base=9000,
            log=global_logger(),
        )

    def prepare_environment(self, build: bool = True) -> None:
        log("\n-- prepare_environment --")
        # build the container image
        if build:
            self.builder.build_image(log=global_logger())
        else:
            log("Skipping build")

        # aggressively clean up anything kvs-related
        # NOTE: this disallows parallel run processes, so turn it off for that
        if not debug: self.conductor.cleanup_hanging(group_only=False)

    def cleanup_environment(self) -> None:
        log("\n-- cleanup_environment --")
        # destroy the cluster
        if not debug: self.conductor.destroy_cluster()
        # aggressively clean up anything kvs-related
        if not debug: self.conductor.cleanup_hanging(group_only=True)


timestamp = datetime.datetime.now().strftime("test_results/%Y_%m_%d_%H:%M:%S")
DEBUG_OUTPUT_DIR = os.path.join(os.getcwd(), timestamp)
os.makedirs(DEBUG_OUTPUT_DIR, exist_ok=True)
log(f"Debug output will be saved in: {DEBUG_OUTPUT_DIR}")


def create_test_dir(base_dir: str, test_set: str, test_name: str) -> str:
    test_set_dir = os.path.join(base_dir, test_set)
    os.makedirs(test_set_dir, exist_ok=True)
    test_dir = os.path.join(test_set_dir, test_name)
    os.makedirs(test_dir, exist_ok=True)
    return test_dir


"""
TEST SET: this list the test cases to run
add more tests by appending to this list
"""
from .tests.hello import hello_cluster
# from .tests.eventual_consistency import EVENTUAL_CONSISTENCY_TESTS
# from .tests.causal_consistency import CAUSAL_CONSISTENCY_TESTS
from .tests.causal_alex import CAUSAL_ALEX_TESTS
from .tests.avail_alex import AVAILABILITY_ALEX_TESTS
from .tests.convergence_alex import CONVERGENCE_TESTS
from .tests.view_change_alex import VIEW_CHANGE_TESTS
from .tests.sharding_basic import BASIC_TESTS
from .tests.shuffle import SHUFFLE_TESTS
from .tests.stress import STRESS_TESTS
from .tests.shard_proxy import PROXY_TESTS
from .tests.bench import BENCHMARKS

TEST_SET = []
TEST_SET.append(TestCase("hello_cluster", hello_cluster))
TEST_SET.extend(AVAILABILITY_ALEX_TESTS)
# TEST_SET.extend(BASIC_TESTS)
TEST_SET.extend(CAUSAL_ALEX_TESTS)
TEST_SET.extend(CONVERGENCE_TESTS)
TEST_SET.extend(BASIC_TESTS)
# TEST_SET.extend(VIEW_CHANGE_TESTS)
TEST_SET.extend(SHUFFLE_TESTS)
TEST_SET.extend(STRESS_TESTS)
TEST_SET.extend(PROXY_TESTS)
# TEST_SET.extend(BENCHMARKS)


# set to True to stop at the first failing test
FAIL_FAST = False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--no-build",
        action="store_false",
        dest="build",
        help="skip building the container image",
    )
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="run all tests instead of stopping at first failure. Note: this is currently broken due to issues with cleanup code",
    )
    parser.add_argument(
        "--num-threads", type=int, default=1, help="number of threads to run tests in"
    )
    parser.add_argument(
        "--port-offset", type=int, default=1000, help="port offset for each test"
    )
    parser.add_argument("filter", nargs="?", help="filter tests by name")
    args = parser.parse_args()

    project_dir = os.getcwd()
    runner = TestRunner(project_dir=project_dir, debug_output_dir=DEBUG_OUTPUT_DIR)
    runner.prepare_environment(build=args.build)

    if args.filter is not None:
        test_filter = args.filter
        log(f"filtering tests by: {test_filter}")
        global TEST_SET
        TEST_SET = [t for t in TEST_SET if re.compile(test_filter).match(t.name)]

    if args.run_all:
        global FAIL_FAST
        FAIL_FAST = False

    log("\n== RUNNING TESTS ==")
    run_tests = []

    def run_test(test: TestCase, gid: str, port_offset: int):
        log(f"\n== TEST: [{test.name}] ==\n")
        test_set_name = test.name.lower().split("_")[0]
        test_dir = create_test_dir(DEBUG_OUTPUT_DIR, test_set_name, test.name)
        log_file_path = os.path.join(test_dir, f"{test.name}.log")

        with open(log_file_path, "w") as log_file:
            log_file.write(f"Logs for test {test.name}\n")

            logger = Logger(files=(log_file, sys.stderr))
            conductor = ClusterConductor(
                group_id=gid,
                base_image=CONTAINER_IMAGE_ID,
                external_port_base=9000 + port_offset,
                log=logger,
            )
            score, reason = test.execute(conductor, test_dir, log=logger)

            # Save logs or any other output to test_dir
            run_tests.append(test)
            logger("\n")
            if score:
                logger(f"✓ PASSED {test.name}")
            else:
                logger(f"✗ FAILED {test.name}: {reason}")
            return score

    if args.num_threads == 1:
        print("Running tests sequentially")
        for test in TEST_SET:
            if not run_test(test, gid="0", port_offset=0):
                if not args.run_all:
                    print("--run-all not set, stopping at first failure")
                    break
    else:
        print("Running tests in a threadpool ({args.num_threads} threads)")
        pool = ThreadPool(processes=args.num_threads)
        pool.map(
            lambda a: run_test(
                a[1], gid=f"{a[0]}", port_offset=a[0] * args.port_offset
            ),
            enumerate(TEST_SET),
        )

    summary_log = os.path.join(DEBUG_OUTPUT_DIR, "summary.log")
    with open(summary_log, "w") as log_file:
        logger = Logger(files=(log_file, sys.stderr))
        logger("\n== TEST SUMMARY ==\n")
        for test in run_tests:
            logger(f"  - {test.name}: {'✓' if test.score else '✗'}\n")

    runner.cleanup_environment()


if __name__ == "__main__":
    main()


