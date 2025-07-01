from ..containers import ClusterConductor
from ..util import log, Logger
from ..kvs_api import KVSClient
from ..testcase import TestCase
from .helper import KVSTestFixture, KVSMultiClient

import time
import random
import string

DEFAULT_TIMEOUT = 10

def make_random_key(prefix="key", length=8):
    chars = string.ascii_lowercase + string.digits
    random_part = "".join(random.choice(chars) for _ in range(length))
    return f"{prefix}{random_part}"


def shard_key_distribution(conductor: ClusterConductor, dir, log: Logger):
    NUM_KEYS = 5000
    NODE_COUNT = 8
    SHARD_COUNT = 4

    with KVSTestFixture(conductor, dir, log, node_count=NODE_COUNT) as fx:
        c = KVSMultiClient(fx.clients, "client", log)

        for i in range(SHARD_COUNT):
            conductor.add_shard(f"shard{i}", conductor.get_nodes([i * 2, i * 2 + 1]))

        fx.broadcast_view(conductor.get_shard_view())

        log(f"\n> ADDING {NUM_KEYS} RANDOM KEYS")
        keys = []
        for i in range(NUM_KEYS):
            key = make_random_key()
            value = key
            node = random.randint(0, NODE_COUNT - 1)

            c.metadata = None
            r = c.put(node, key, value)
            assert r.ok, f"expected ok for {key}, got {r.status_code}"
            keys.append(key)

            if (i + 1) % 50 == 0:
                log(f"Added {i + 1} keys")

        log("\n> CHECKING KEY DISTRIBUTION")
        shard_key_counts = {}

        for shard_idx in range(SHARD_COUNT):
            node_id = shard_idx * 2

            c.metadata = None

            r = c.get_all(node_id)
            assert r.ok, (
                f"expected ok for get_all from shard {shard_idx}, got {r.status_code}"
            )

            shard_keys = r.json().get("items", {})
            shard_key_counts[shard_idx] = len(shard_keys)
            log(f"Shard {shard_idx} has {len(shard_keys)} keys")

            # randomly sample keys to verify
            for key in random.sample(list(shard_keys.keys()), min(10, len(shard_keys))):
                c.metadata = None
                r = c.get(node_id, key)
                assert r.ok, f"expected ok for get {key}, got {r.status_code}"
                assert r.json()["value"] == shard_keys[key], (
                    f"wrong value returned for {key}"
                )

        if shard_key_counts:
            avg_keys = sum(shard_key_counts.values()) / len(shard_key_counts)
            min_keys = min(shard_key_counts.values())
            max_keys = max(shard_key_counts.values())
            deviation = max_keys - min_keys
            deviation_percent = (deviation / avg_keys) * 100 if avg_keys > 0 else 0

            log(f"Key distribution: min={min_keys}, max={max_keys}, avg={avg_keys:.1f}")
            log(f"Max deviation: {deviation} keys ({deviation_percent:.1f}%)")

            is_good_distribution = deviation_percent < 40

            return (
                is_good_distribution,
                f"Key distribution test completed. Deviation: {deviation_percent:.1f}%",
            )

        return False, "Could not collect key distribution data"


def shard_addition_performance(conductor: ClusterConductor, dir, log: Logger):
    NUM_KEYS = 800
    NODE_COUNT = 22
    INITIAL_SHARDS = 10

    with KVSTestFixture(conductor, dir, log, node_count=NODE_COUNT) as fx:
        c = KVSMultiClient(fx.clients, "client", log)

        for i in range(INITIAL_SHARDS):
            conductor.add_shard(f"shard{i}", conductor.get_nodes([i * 2, i * 2 + 1]))

        fx.broadcast_view(conductor.get_shard_view())

        log(f"\n> ADDING {NUM_KEYS} RANDOM KEYS")
        key_values = {}
        for i in range(NUM_KEYS):
            key = make_random_key()
            value = key
            # node = random.randint(0, NODE_COUNT-3)
            node = random.randint(0, NODE_COUNT - 3)

            c.metadata = None

            r = c.put(node, key, value)
            assert r.ok, f"expected ok for {key}, got {r.status_code}"
            key_values[key] = value

            if (i + 1) % 50 == 0:
                log(f"Added {i + 1} keys")

        log("\n> CHECKING INITIAL KEY DISTRIBUTION")
        initial_distribution = {}

        for i in range(INITIAL_SHARDS):
            node_id = i * 2

            c.metadata = None

            r = c.get_all(node_id)
            assert r.ok, f"expected ok for get_all from shard {i}, got {r.status_code}"
            shard_keys = r.json().get("items", {})
            initial_distribution[i] = set(shard_keys.keys())
            log(f"Shard {i} initially has {len(shard_keys)} keys")

        log("\n> ADDING NEW SHARD")

        conductor.shards = dict(conductor.shards)
        conductor.add_shard("newShard", conductor.get_nodes([20, 21]))
        fx.broadcast_view(conductor.get_shard_view())

        log("Waiting for resharding to complete...")
        time.sleep(DEFAULT_TIMEOUT)

        log("\n> CHECKING KEY DISTRIBUTION AFTER RESHARDING")
        final_distribution = {}
        total_keys_after = 0

        for i in range(INITIAL_SHARDS):
            node_id = i * 2
            c.metadata = None
            r = c.get_all(node_id)
            assert r.ok, f"expected ok for get_all from shard {i}, got {r.status_code}"
            shard_keys = r.json().get("items", {})
            final_distribution[i] = set(shard_keys.keys())
            total_keys_after += len(shard_keys)
            log(f"Shard {i} now has {len(shard_keys)} keys")

        c.metadata = None
        r = c.get_all(20)
        assert r.ok, f"expected ok for get_all from new shard, got {r.status_code}"
        new_shard_keys = r.json().get("items", {})
        final_distribution["new"] = set(new_shard_keys.keys())
        total_keys_after += len(new_shard_keys)
        log(f"New shard has {len(new_shard_keys)} keys")

        keys_moved = 0
        for shard_idx, initial_keys in initial_distribution.items():
            final_keys = final_distribution[shard_idx]
            moved_from_this_shard = len(initial_keys - final_keys)
            keys_moved += moved_from_this_shard
            log(f"Shard {shard_idx} lost {moved_from_this_shard} keys")

        # should move roughly 1/N+1 keys
        expected_keys_moved = NUM_KEYS / (INITIAL_SHARDS + 1)
        actual_moved_ratio = keys_moved / NUM_KEYS
        expected_moved_ratio = 1 / (INITIAL_SHARDS + 1)

        log(
            f"Expected keys to move: ~{expected_keys_moved:.1f} ({expected_moved_ratio * 100:.1f}%)"
        )
        log(f"Actual keys moved: {keys_moved} ({actual_moved_ratio * 100:.1f}%)")

        is_efficient = actual_moved_ratio <= expected_moved_ratio * 1.5

        return (
            is_efficient,
            f"Shard addition test completed. Keys moved: {keys_moved}/{NUM_KEYS} ({actual_moved_ratio * 100:.1f}%), "
            + f"Efficiency: {'good' if is_efficient else 'poor'}",
        )


def shard_removal_performance(conductor: ClusterConductor, dir, log: Logger):
    NUM_KEYS = 800
    NODE_COUNT = 22
    INITIAL_SHARDS = 11

    with KVSTestFixture(conductor, dir, log, node_count=NODE_COUNT) as fx:
        c = KVSMultiClient(fx.clients, "client", log)

        for i in range(INITIAL_SHARDS):
            conductor.add_shard(f"shard{i}", conductor.get_nodes([i * 2, i * 2 + 1]))

        fx.broadcast_view(conductor.get_shard_view())

        log(f"\n> ADDING {NUM_KEYS} RANDOM KEYS")
        key_values = {}
        for i in range(NUM_KEYS):
            key = make_random_key()
            value = key
            node = random.randint(0, NODE_COUNT - 1)

            c.metadata = None

            r = c.put(node, key, value)
            assert r.ok, f"expected ok for {key}, got {r.status_code}"
            key_values[key] = value

            if (i + 1) % 50 == 0:
                log(f"Added {i + 1} keys")

        log("\n> CHECKING INITIAL KEY DISTRIBUTION")
        initial_distribution = {}
        total_keys_before = 0

        for i in range(INITIAL_SHARDS):
            node_id = i * 2

            c.metadata = None

            r = c.get_all(node_id)
            assert r.ok, f"expected ok for get_all from shard {i}, got {r.status_code}"
            shard_keys = r.json().get("items", {})
            initial_distribution[i] = set(shard_keys.keys())
            total_keys_before += len(shard_keys)
            log(f"Shard {i} initially has {len(shard_keys)} keys")

        shard_to_remove = "shard5"
        shard_idx_to_remove = 5
        log(f"\n> REMOVING SHARD: {shard_to_remove}")

        removed_shard_keys = initial_distribution[shard_idx_to_remove]
        log(f"The removed shard had {len(removed_shard_keys)} keys")

        nodes_to_reassign = conductor.shards[shard_to_remove]
        log(f"Moving node {nodes_to_reassign[0]} to shard0")

        conductor.shards = dict(conductor.shards)
        conductor.shards["shard0"] = conductor.shards["shard0"] + [nodes_to_reassign[0]]
        del conductor.shards[shard_to_remove]
        fx.broadcast_view(conductor.get_shard_view())

        log("Waiting for resharding to complete...")
        time.sleep(DEFAULT_TIMEOUT)

        log("\n> CHECKING KEY DISTRIBUTION AFTER RESHARDING")
        final_distribution = {}
        total_keys_after = 0

        for i in range(INITIAL_SHARDS):
            if i == shard_idx_to_remove:
                continue

            node_id = i * 2
            c.metadata = None
            r = c.get_all(node_id)
            assert r.ok, f"expected ok for get_all from shard {i}, got {r.status_code}"
            shard_keys = r.json().get("items", {})
            final_distribution[i] = set(shard_keys.keys())
            total_keys_after += len(shard_keys)
            log(f"Shard {i} now has {len(shard_keys)} keys")

        keys_redistributed = len(removed_shard_keys)

        if total_keys_after < total_keys_before:
            log(
                f"WARNING: Some keys may have been lost. Before: {total_keys_before}, After: {total_keys_after}"
            )

        redistributed_keys_per_shard = {}
        for i in final_distribution:
            if i in initial_distribution:
                new_keys = len(final_distribution[i] - initial_distribution[i])
                redistributed_keys_per_shard[i] = new_keys
                log(f"Shard {i} received {new_keys} new keys")

        values = list(redistributed_keys_per_shard.values())
        if values:
            avg_keys_received = sum(values) / len(values)
            max_deviation = max(abs(v - avg_keys_received) for v in values)
            deviation_percent = (
                (max_deviation / avg_keys_received) * 100
                if avg_keys_received > 0
                else 0
            )

            log(
                f"Expected keys redistributed per shard: ~{keys_redistributed / (INITIAL_SHARDS - 1):.1f}"
            )
            log(
                f"Actual redistribution: max deviation {deviation_percent:.1f}% from average"
            )

            is_efficient = deviation_percent < 150

            return (
                is_efficient,
                f"Shard removal test completed. {keys_redistributed} keys redistributed, "
                + f"Efficiency: {'good' if is_efficient else 'poor'}",
            )

        return (False, "Could not properly analyze key redistribution")


STRESS_TESTS = [
    TestCase("shard_key_distribution", shard_key_distribution),
    TestCase("shard_addition_performance", shard_addition_performance),
    TestCase("shard_removal_performance", shard_removal_performance),
]

