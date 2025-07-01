# Distributed Key-Value Store

This repository contains two progressively more advanced implementations of a distributed key-value store, each exploring different consistency models and scalability strategies in distributed systems.

## Overview

- 🧱 `strong_consistency`: Implements **strong consistency** using **Primary-Backup Replication**. This design ensures that all clients observe a single, globally consistent state — even during concurrent operations. However, it sacrifices **availability during network partitions**, following the [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem).

- ⚙️ `sharded_causal_kv_store`: Implements a **causally consistent**, **eventually consistent**, and **highly available** key-value store. This system supports **horizontal and vertical scaling**, is resilient under node churn and partitions, and achieves high performance in distributed deployments.

## 🔍 Highlights by Project

### ✅ `strong_consistency/`
- **Consistency model**: Strong consistency (linearizability)
- **Replication strategy**: Primary-Backup Replication
- **Technologies**: Flask, Docker
- **Trade-offs**:
  - Loses availability during partitions (CP system in CAP)

### 🚀 `sharded_causal_kv_store/`
- **Consistency model**: Causal + eventual consistency
- **Replication strategy**: Broadcast-based replication with gossip protocol for eventual convergence
- **Scalability**: 
  - **Horizontal scaling** via **consistent hashing** (key partitioning across shards)
  - **Automatic resharding** on view changes (node joins/leaves)
- **Fault Tolerance**: Handles node churn, view changes, and network partitions
- **Causality**: Uses **vector clocks** to preserve cause-effect relationships across nodes and shards
- **Technologies**: FastAPI, Docker
- **Trade-offs**:
  - Does not guarantee complete atomicity, unlike strongly consistent systems, in concurrent operations

## 🧪 Testing & Deployment

- Projects were tested using a distributed systems benchmarking suite that simulates:
  - Network partitions
  - Concurrent clients
  - View changes (adding/removing nodes)
- Both systems were deployed in multi-container Docker environments for distributed testing.

## 🧠 Key Learnings

- Trade-offs between **consistency**, **availability**, and **network partition tolerance**
- Building fault-tolerant systems with **causal ordering guarantees**
- Designing scalable distributed architectures using **sharding and hashing**
- Applying practical protocols like **vector clocks**, **gossip**, and **broadcast replication**

---