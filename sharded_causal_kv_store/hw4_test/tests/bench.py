from ..containers import ClusterConductor
from ..util import log, Logger
from ..kvs_api import KVSClient
from ..testcase import TestCase
from .helper import KVSTestFixture, KVSMultiClient

import time
import asyncio

import matplotlib.pyplot as plt

NUM_SHARDS = 20
NUM_KEYS = 500
NUM_NODES = 8

def benchmark_add_shard(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=NUM_SHARDS) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0]))
        fx.broadcast_view(conductor.get_shard_view())

        log(f"putting {NUM_KEYS} keys\n")
        put_times = []
        for i in range(NUM_KEYS):
            c = KVSMultiClient(fx.clients, "client", log)
            start_time = time.time()
            r = c.put(0, f"key{i}", f"value{i}", timeout=10)
            end_time = time.time()
            assert r.ok, f"expected ok for new key, got {r.status_code}"
            put_times.append(end_time - start_time)

        log("Starting benchmark\n")
        reshard_times = []
        for shard in range(2, NUM_SHARDS+1):
            start_time = time.time()
            log(f"adding shard{shard}\n")
            conductor.add_shard(f"shard{shard}", conductor.get_nodes([shard - 1]))
            asyncio.run(fx.parallel_broadcast_view(conductor.get_shard_view()))
            end_time = time.time()
            reshard_times.append(end_time - start_time)
            log(f"reshard time with {shard} shards: {reshard_times[-1]}\n")

    log("Average put time: ", sum(put_times) / len(put_times))
    for shard, time_taken in enumerate(reshard_times, start=2):
        log(f"shard count: {shard}, reshard time: {time_taken}")

    # Generate plot
    plt.figure(figsize=(NUM_SHARDS, 10))
    plt.plot(range(2, NUM_SHARDS+1), reshard_times, marker='o')
    plt.title('Reshard Times')
    plt.xlabel('Number of Shards')
    plt.ylabel('Time (seconds)')
    plt.grid(True)
    plt.savefig(f"{dir}/reshard_times.png")

    return True, "ok"

def benchmark_add_shard_two_nodes(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=NUM_SHARDS*NUM_NODES) as fx:
        conductor.add_shard("shard1", conductor.get_nodes([0, 1]))
        fx.broadcast_view(conductor.get_shard_view())

        log(f"putting {NUM_KEYS} keys\n")
        put_times = []
        for i in range(NUM_KEYS):
            c = KVSMultiClient(fx.clients, "client", log)
            start_time = time.time()
            r = c.put(i%NUM_NODES, f"key{i}", f"value{i}", timeout=10)
            end_time = time.time()
            assert r.ok, f"expected ok for new key, got {r.status_code}"
            put_times.append(end_time - start_time)

        log("Starting benchmark\n")
        reshard_times = []
        for shard in range(2, NUM_SHARDS+1):
            start_time = time.time()
            log(f"adding shard{shard}\n")
            conductor.add_shard(f"shard{shard}", conductor.get_nodes([2*(shard - 1), 2*(shard - 1) + 1]))
            asyncio.run(fx.parallel_broadcast_view(conductor.get_shard_view()))
            end_time = time.time()
            reshard_times.append(end_time - start_time)
            log(f"reshard time with {shard} shards: {reshard_times[-1]}\n")

    log("Average put time: ", sum(put_times) / len(put_times))
    for shard, time_taken in enumerate(reshard_times, start=2):
        log(f"shard count: {shard}, reshard time: {time_taken}")

    # Generate plot
    plt.figure(figsize=(NUM_SHARDS, 10))
    plt.plot(range(2, NUM_SHARDS+1), reshard_times, marker='o')
    plt.title('Reshard Times')
    plt.xlabel('Number of Shards')
    plt.ylabel('Time (seconds)')
    plt.grid(True)
    plt.savefig(f"{dir}/reshard_times.png")

    return True, "ok"

BENCHMARKS = [TestCase("benchmark_add_shard", benchmark_add_shard),
              TestCase("benchmark_add_shard_two_nodes", benchmark_add_shard_two_nodes)]
