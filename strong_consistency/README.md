# Strongly Consistent Distributed Key-Value Store

**Project directory:** `strong-consistency/`

A Python/Flask-based distributed key-value store that guarantees **strong consistency** (linearizability) using a **Primary-Backup Replication** protocol. This system ensures that all replicas maintain the same state, and that all operations appear to execute atomically in a global total order — even when handled by different nodes.

---

## 🔐 Key Properties

- **🧠 Linearizable Reads & Writes**  
  Ensures operations appear instantaneously at a single point in global time. All clients observe the same order of updates.

- **🗂 Primary-Backup Replication**  
  The primary node serializes all operations and broadcasts updates synchronously to backups before acknowledging writes.

- **🧱 Strong Durability (In-Memory)**  
  No client receives an acknowledgment until all replicas have applied the write. As long as one node survives, no data is lost.

- **🔁 Fast Failover Support**  
  External processes can reconfigure views to promote a surviving backup to primary during failure scenarios.

- **🌐 Simple RESTful API**  
  Exposes HTTP endpoints for client operations (`/data`, `/data/<key>`) and cluster view configuration (`/view`).

> ⚠️ **Note on Availability:**  
To preserve strong consistency, the system prioritizes consistency over availability during partitions (i.e., it's a **CP system** in the CAP theorem). If the primary cannot reach backups, it will reject writes.

---

## 🧱 Architecture

```text
          ┌───────────────┐        PUT / GET / DELETE
Clients ──▶ Primary Node  │
          │  Flask app    │◀─── Broadcast replication ───▶ Backup Nodes (n ≥ 1)
          └───────────────┘        (HTTP)

```

## 🧪 REST API

This key-value store exposes the following endpoints:

### `GET /ping`
- **Purpose**: Health check endpoint.

- **Response**: `200 OK` if the node is initialized and ready.

### `PUT /data/<key>`

- **Body**:
    ```json
    {
    "value": "some string"
    }
    ```
- **Purpose**: Creates or updates a key-value pair.

- **Returns**:

    - `201 Created` if the key is new

    - `200 OK` if the key is updated

- Error: `400 Bad Request` if the body is missing from the PUT request or isn’t valid json in the form expected

### `GET /data/<key>`
- **Purpose**: Returns the value associated with the key

- **Returns**:

    - `200 OK` with:

        ```json
        { "value": "..." }
        ```
    - `404 Not Found` if the key doesn't exist

### `DELETE /data/<key>`
- **Purpose**: Deletes the key if it exists

- **Returns**:

    - `200 OK` if the key existed and was deleted

    - `404 Not Found` if the key was not present

### `GET /data`
- **Purpose**: Returns a full key-value snapshot of the key-value store

- **Returns**:
    ```json
    {
        "key1": "value1",
        "key2": "value2",
        ...
    }
    ```

### `PUT /view`
- **Body**:
    ```json
    {
        "view": [
            { "address": "172.4.0.1:8081", "id": 1 },
            { "address": "172.4.0.2:8081", "id": 2 },
            ...
        ]
    }
    ```
- **Purpose**: Sent to all nodes; updating each node with information on the nodes that are currently active

- **Returns**: `200 OK` once view is acknowledged

## ⚙️ Development Setup

You can quickly spin up a cluster for testing using the provided `devenv.py` script:

```bash
python devenv.py -n <number_of_nodes>