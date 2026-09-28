# MeshWeaver

**Zero-Heavy-Dependency P2P Asynchronous Task Broker built with Python, `asyncio`, and UDP.**

MeshWeaver is a decentralized peer-to-peer compute mesh in which independent Python processes discover one another, exchange health and resource information, and execute distributed workloads without a centralized broker.

It combines **Kademlia-style peer discovery**, **anti-entropy gossip**, **heartbeat-based failure detection**, **load-aware task scheduling**, **automatic task rerouting**, and **secure message authentication** into a lightweight Python-based distributed task system.

> **Status:** Stable v0.1.0  
> **Python:** 3.11+  
> **License:** Not yet selected

---

## Why MeshWeaver?

Traditional distributed task queues often depend on centralized infrastructure such as Redis, RabbitMQ, Celery workers, or external databases.

MeshWeaver explores a different architecture:

- No central broker
- No master node
- Peer-to-peer node discovery
- Real OS-level UDP networking
- Load-aware task routing
- Failure detection and rerouting
- Remote task management
- Structured task arguments
- HMAC-SHA256 message authentication
- Terminal-based observability
- Automated and real-network verification

The goal is a **lightweight, decentralized task execution mesh** suitable for distributed-systems experimentation and edge-computing scenarios.

---

## Architecture

```text
                         ┌─────────────────────┐
                         │    Client / CLI      │
                         └──────────┬──────────┘
                                    │
                              Submit Task
                                    │
                                    ▼
                    ┌──────────────────────────────┐
                    │       Coordinator Node       │
                    │                              │
                    │  Kademlia DHT / Peer Table   │
                    │  Gossip / Resource Metrics   │
                    │  Heartbeat / Failure Detector │
                    │  Load-Aware Scheduler        │
                    └──────────────┬───────────────┘
                                   │
                         UDP Task Dispatch
                                   │
                   ┌───────────────┴───────────────┐
                   ▼                               ▼
          ┌─────────────────┐             ┌─────────────────┐
          │   Worker Node B │             │   Worker Node C │
          │                 │             │                 │
          │ Task Router     │             │ Task Router     │
          │ Thread Executor │             │ Thread Executor │
          │ Result Handler  │             │ Result Handler  │
          └────────┬────────┘             └────────┬────────┘
                   │                               │
                   └──────────────┬────────────────┘
                                  │
                              Task Result
                                  │
                                  ▼
                         Coordinator / Client
```

### Core Components

| Component | Responsibility | Implementation |
|---|---|---|
| **UDP Network** | Non-blocking node communication | `asyncio.DatagramProtocol` |
| **Kademlia DHT** | Peer discovery and routing | Node IDs, XOR distance, k-buckets |
| **Gossip Engine** | Resource/topology dissemination | Periodic randomized fan-out |
| **Heartbeat Monitor** | Failure detection | Miss thresholds and peer state |
| **Scheduler** | Worker selection | CPU/RAM-aware routing |
| **Fault Tolerance** | Recovery | Retry and task rerouting |
| **Serializer** | Remote Python execution | `cloudpickle` |
| **Security** | Message authentication | HMAC-SHA256 |
| **Dashboard** | Observability | Terminal UI |

---

## Key Features

### Distributed Networking
- Peer-to-peer UDP communication
- Multi-node discovery
- Kademlia-style DHT routing
- Transitive peer discovery
- Robust bootstrap handshake
- Real operating-system sockets

### Monitoring & Reliability
- CPU and RAM telemetry
- Gossip-based metric dissemination
- Heartbeat-based liveness detection
- `ALIVE` / `DEAD` peer states
- Automatic worker exclusion after failure
- Automatic task retry and rerouting

### Distributed Task Execution
- Remote Python function execution
- `cloudpickle` serialization
- Nested functions and closures
- Functions defined in `__main__`
- Thread-pool execution
- Task lifecycle tracking
- Remote task cancellation
- Graceful remote node shutdown

### CLI
- Node management
- Peer inspection
- Task submission
- Task status
- Task cancellation
- Structured JSON arguments
- Remote node shutdown
- Terminal monitoring

### Security
- HMAC-SHA256 message authentication
- Envelope validation
- Tamper detection
- No use of `eval` for structured CLI arguments

---

## Technology Stack

- **Python:** 3.11+
- **Concurrency:** `asyncio`
- **Networking:** UDP / `asyncio.DatagramProtocol`
- **Serialization:** `cloudpickle`
- **Telemetry:** `psutil` with fallback behavior
- **Testing:** `pytest`, `pytest-asyncio`
- **Linting:** `ruff`
- **Packaging:** PEP 517 / PEP 621

MeshWeaver follows a **zero-heavy-dependency** philosophy. It does not require infrastructure such as Redis, RabbitMQ, or Celery.

---

# Installation

## 1. Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd MeshWeaver
```

## 2. Create a Virtual Environment

### Windows

```cmd
python -m venv .venv
.venv\Scripts\activate
```

### Linux / macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 3. Install

```bash
pip install -e ".[dev]"
```

After installation, the `meshweaver` CLI becomes available in the active environment.

---

# Quick Start

## Start Node A

Open **Terminal 1**:

```bash
python -m meshweaver.cli node start --node-id node_a --host 127.0.0.1 --port 5000
```

## Start Node B

Open **Terminal 2**:

```bash
python -m meshweaver.cli node start --node-id node_b --host 127.0.0.1 --port 5001 --bootstrap 127.0.0.1:5000
```

## Start Node C

Open **Terminal 3**:

```bash
python -m meshweaver.cli node start --node-id node_c --host 127.0.0.1 --port 5002 --bootstrap 127.0.0.1:5001
```

Node C can bootstrap through Node B and learn about the existing mesh through peer discovery.

---

# Verify Connectivity

## Ping a Worker

```bash
python -m meshweaver.cli ping --host 127.0.0.1 --port 5001
```

Expected:

```text
PONG received — peer is reachable
```

## View Node Status

```bash
python -m meshweaver.cli node status --host 127.0.0.1 --port 5000
```

Example:

```text
node_id : node_a
state   : RUNNING
address : 127.0.0.1:5000
peers   : 2
tasks   : {'pending': 0, 'running': 0, 'completed': 3, 'failed': 0,
           'cancelled': 0, 'total': 3}
```

## View Peers

```bash
python -m meshweaver.cli node peers --host 127.0.0.1 --port 5000
```

Example:

```text
node_b    127.0.0.1:5001    status=ALIVE
node_c    127.0.0.1:5002    status=ALIVE
```

---

# Task Execution

## 1. Explicit Worker Targeting

Submit a task directly to a worker:

```bash
python -m meshweaver.cli submit ^
  --target node_b ^
  --target-host 127.0.0.1 ^
  --target-port 5001 ^
  --function examples.simple_task:add ^
  --args 100 200
```

Expected:

```text
Task ID: <TASK_ID>
Target: node_b
Status: COMPLETED
Result: 300
```

---

## 2. Automatic Load-Aware Scheduling

Let MeshWeaver select a worker based on the available peer information:

```bash
python -m meshweaver.cli submit ^
  --host 127.0.0.1 ^
  --port 5000 ^
  --function examples.simple_task:add ^
  --args 100 250
```

Example:

```text
Task ID: <TASK_ID>
Target: node_b
Status: COMPLETED
Result: 350
```

---

## 3. Complex Arguments

MeshWeaver supports JSON-encoded arguments:

```bash
python -m meshweaver.cli submit ^
  --target node_b ^
  --target-host 127.0.0.1 ^
  --target-port 5001 ^
  --function examples.complex_task:word_frequency ^
  --args-json "[\"hello meshweaver hello world\"]"
```

Example result:

```text
Status: COMPLETED
Result: {'hello': 2, 'meshweaver': 1, 'world': 1}
```

---

## 4. Query Task Status

```bash
python -m meshweaver.cli task status ^
  --host 127.0.0.1 ^
  --port 5001 ^
  --task-id <TASK_ID>
```

Example:

```text
task_id : <TASK_ID>
state   : COMPLETED
target  : node_b
result  : 300
```

---

## 5. Cancel a Task

```bash
python -m meshweaver.cli task cancel ^
  --host 127.0.0.1 ^
  --port 5001 ^
  --task-id <TASK_ID>
```

---

## 6. Stop a Remote Node

```bash
python -m meshweaver.cli node stop ^
  --host 127.0.0.1 ^
  --port 5002
```

---

# Example Workloads

MeshWeaver includes deterministic example workloads:

```text
examples/
├── simple_task.py
├── complex_task.py
└── ml_task.py
```

Examples include:

- Arithmetic operations
- Word-frequency analysis
- Prime-number generation
- Linear regression
- Clustering workloads

---

# Monitoring

Inspect the current peer and task state:

```bash
python -m meshweaver.cli node peers --host 127.0.0.1 --port 5000
```

A terminal dashboard is also available when supported by the installed project configuration.

Example:

```text
============================================================
 MESHWEAVER
============================================================
 Node: node_a @ 127.0.0.1:5000
 State: RUNNING

 Tasks
   Total:     12
   Running:    1
   Completed: 10
   Failed:     1
   Cancelled:  0

 Peers
   node_b  127.0.0.1:5001  ALIVE
   node_c  127.0.0.1:5002  ALIVE
============================================================
```

---

# Security Model

MeshWeaver deliberately documents its security boundaries.

## HMAC-SHA256 Authentication

Messages can be authenticated using HMAC-SHA256.

This provides:

- Message authenticity
- Tamper detection
- Protection against unauthorized message injection when peers share the secret

**HMAC does not encrypt network traffic.**

## `cloudpickle` Is Not a Sandbox

`cloudpickle` allows Python functions and executable objects to be transmitted between nodes.

Therefore:

> Only execute workloads received from trusted/authenticated peers.

A remote task executes with the operating-system permissions of the MeshWeaver process.

## Transport Encryption

The default transport is UDP and is not encrypted.

For production deployments on untrusted networks, use an encrypted overlay or secure network boundary such as:

- WireGuard
- IPsec
- Authenticated VPC/network segmentation

---

# Testing & Verification

MeshWeaver uses two levels of verification.

## Automated Test Suite

```bash
python -m pytest -q
```

Current verified baseline:

```text
92 passed
```

## Real Network Verification

Run:

```bash
python verify_all.py
```

Current release verification:

```text
20 / 20 real-network verification checks passed
```

The verification covers real multi-process UDP communication, peer discovery, task execution, routing, failure scenarios, and CLI behavior.

---

# Project Structure

```text
MeshWeaver/
├── meshweaver/
│   ├── __init__.py
│   ├── cli.py
│   ├── config.py
│   ├── dht.py
│   ├── executor.py
│   ├── gossip.py
│   ├── heartbeat.py
│   ├── logging_config.py
│   ├── monitor.py
│   ├── network.py
│   ├── node.py
│   ├── peer.py
│   ├── protocol.py
│   ├── router.py
│   ├── scheduler.py
│   ├── security.py
│   ├── serializer.py
│   └── dashboard.py
│
├── tests/
├── docs/
├── examples/
│
├── pyproject.toml
├── requirements.txt
├── CHANGELOG.md
├── CONTRIBUTING.md
├── app.py
└── verify_all.py
```

---

# Design Decisions

### UDP

UDP was selected for lightweight peer communication, heartbeats, gossip, and distributed control messages.

Request/response correlation and application-level reliability mechanisms are implemented above the datagram layer where required.

### Decentralized Architecture

There is no permanent master or central broker.

A node can participate as:

- Coordinator
- Worker
- Peer
- Task client

### Load-Aware Scheduling

Worker selection considers peer telemetry to avoid blindly sending workloads to unavailable or overloaded nodes.

### Thread-Pool Execution

Task execution is moved away from the main `asyncio` event loop so that networking, heartbeat monitoring, and gossip remain responsive.

---

# Engineering Highlights

MeshWeaver demonstrates practical distributed-systems concepts rather than being only a local task-execution demo:

- Peer-to-peer networking
- UDP socket programming
- Async concurrency
- Distributed peer discovery
- Kademlia-style routing
- Gossip protocols
- Failure detection
- Fault-tolerant task routing
- Remote execution
- Serialization
- HMAC authentication
- CLI/API design
- Structured logging
- Automated testing
- Multi-process real-network testing
- Packaging and release management

---

# Release

## v0.1.0 — Initial Stable Release

MeshWeaver v0.1.0 represents the first stable release of the project.

### Release verification

- **92 automated tests passed**
- **20/20 real-network verification checks passed**
- P2P UDP communication verified
- Multi-node discovery verified
- Task execution verified
- Task status tracking verified
- Load-aware routing verified
- Failure handling verified
- CLI workflows verified

---

# Roadmap

Future development may include:

- Optional TCP/TLS transport adapter
- Improved distributed result persistence
- Dynamic worker-pool scaling
- Larger-scale network benchmarking
- More advanced scheduling policies
- Persistent distributed task history
- Additional observability and metrics

---

# Contributing

Contributions, issues, and engineering discussions are welcome.

Before submitting changes:

```bash
python -m pytest -q
```

Please ensure that new functionality includes appropriate tests and documentation.

---

# License

A license has not yet been selected for this repository.

---

## Author

**Md Kamreaj Alam**

B.Tech Computer Science (AI)  
Interested in **AI/ML, distributed systems, Python, and AI engineering**.

---

> MeshWeaver is a learning and engineering project focused on building a decentralized asynchronous task-execution system using Python and low-level networking primitives.
