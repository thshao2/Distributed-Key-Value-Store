# Strong Consistency Distributed Key-Value Store

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

## ⚙️ Development Setup

You can quickly spin up a cluster for testing using the provided `devenv.py` script:

```bash
python devenv.py -n <number_of_nodes>