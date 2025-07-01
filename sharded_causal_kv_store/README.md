# Highly Available, Causally Consistent Distributed Key-Value Store

**Project directory:** `sharded-causal-kv-store/`

A scalable, fault-tolerant, and highly-available key-value store that provides **causal consistency** and **eventual convergence**. Built in Python using **FastAPI**, the system achieves **horizontal scalability** via **consistent hashing and sharding**, and supports **vertical scalability** through a **gossip-based anti-entropy protocol** and **vector clock-based causal tracking**.

## 🔍 Why Causal Consistency?

In distributed systems, strong consistency often sacrifices availability during network partitions. Causal consistency strikes a practical balance: it preserves *cause-and-effect* relationships across operations, while allowing availability and performance under partial failures.

This means:
- Clients always observe effects **only after their causes**.
- Unrelated operations may be seen in different orders across replicas.
- The system stays available for reads/writes even under network partitions.

---

## ✨ Features

- **🧠 Causal Consistency**  
  Maintains *happens-before* relationships between dependent operations using vector clocks. Operations with no dependency can be applied in any order, as long as all replicas agree.

- **🕓 Eventual Convergence**  
  After all operations cease and partitions heal, all replicas reach a consistent state within a bounded time window (10 seconds).

- **🌐 High Availability**  
  As long as **one** replica in the correct shard is reachable, reads and writes can succeed — without sacrificing causal correctness.

- **🧩 Sharding via Consistent Hashing**  
  Distributes the key space across shards using consistent hashing with virtual nodes. This supports dynamic scaling and balanced data placement.

- **🔁 Automatic Resharding**  
  When the view changes (e.g., nodes are added/removed), data is automatically transferred to the appropriate shards. On average, only ≈ K/S keys are moved per event (where K = number of keys, S = number of shards).

- **📡 Gossip-based Anti-Entropy**  
  Periodically exchanges metadata between nodes to ensure all updates propagate efficiently and converge across the system.

- **🔌 Simple RESTful Interface**  
  Provides endpoints for reading, writing, deleting, and managing the distributed cluster state.

---

## 🧪 REST API

This key-value store exposes the following endpoints:

### `GET /ping`
- **Purpose**: Health check endpoint.

- **Response**: `200 OK` if the node is initialized and ready.

### `PUT /data/<key>`

- **Body**:
    ```json
    {
    "value": "some string value",
    "causal-metadata": { ... }
    }
    ```
- **Purpose**: Creates or updates a key-value pair, propagating causal metadata.

- **Returns**:

    - `200 OK` with updated `causal-metadata`

- **Response**:
    ```json
    {
    "causal-metadata": { ... }
    }
    ```

- Error: `400 Bad Request` if the body is missing from the PUT request or isn’t valid json in the form expected

*Requests for keys not belonging to the local shard are transparently forwarded to the correct node.*

### `GET /data/<key>`
- **Purpose**: Returns the value associated with the key, respecting causal dependencies.

- **Body**:
    ```json
    {
    "causal-metadata": { ... }
    }
    ```
- **Response**:
    ```json
    {
    "value": "some string value",
    "causal-metadata": { ... }
    }
    ```

- **Returns**:

    - `200 OK` if key exists
    - `404 Not Found` if the key doesn't exist (with unchanged causal metadata)


### `GET /data`
- **Purpose**:  Returns all keys currently stored *on this node’s shard* that are causally safe to return.

- **Body**:
    ```json
    {
    "causal-metadata": { ... }
    }
    ```

- **Response**:
    ```json
    {
        "items": {
            "key1": "val1",
            "key2": "val2",
            ...
        },
        "causal-metadata": { ... }
    }
    ```

### `PUT /view`
- **Body**:
    ```json
    {
        "view": {
            "Shard1": [
            { "address": "172.4.0.4:8081", "id": 4 },
            { "address": "172.4.0.2:8081", "id": 2 }
            ],
            "Shard2": [
            { "address": "172.4.0.3:8081", "id": 3 }
            ]
        }
    }
    ```
- **Purpose**: Updates the cluster view to a new sharded configuration

- **Returns**: `200 OK` once the node is ready to serve the new view

- **Effect**:
    - Triggers resharding of data across the system.

    - All nodes involved must acknowledge the change before requests resume.

## ⚙️ Development Setup

You can quickly spin up a cluster for testing using the provided `devenv.py` script:

```bash
python devenv.py -n <number_of_nodes>
