# MeshWeaver

**Zero-Heavy-Dependency P2P Asynchronous Task Broker built with Python, asyncio, and UDP.**

MeshWeaver is a decentralized peer-to-peer compute mesh where multiple independent Python processes discover each other via a Kademlia-style Distributed Hash Table (DHT), disseminate system metrics via anti-entropy gossip, monitor cluster health through heartbeat failure detectors, and dispatch arbitrary Python workloads with automatic failover and rerouting.

Designed with a strict **zero-heavy-dependency** philosophy, MeshWeaver relies solely on the Python Standard Library, `cloudpickle` (for closure and interactive function serialization), and optional `psutil` (for host telemetry).

---

## Architecture Overview

MeshWeaver operates as a fully peer-to-peer network where any node can act simultaneously as a client, task coordinator, and compute worker. There are no central brokers, masters, or external databases.

```text
                           TASK DISPATCH LIFECYCLE
                           
           Client (CLI / API)
                 │
                 │ submit_task(func, args)
                 ▼
        ┌─────────────────────────────────────────────────┐
        │  Coordinator Node                               │
        │                                                 │
        │   DHT / Routing Table ──► Discovers active peers│
        │   Gossip Protocol     ──► Live CPU & RAM loads  │
        │   Heartbeat Monitor   ──► Liveness & dead nodes │
        │                                                 │
        │   [ Scheduler ] ────────────────────────────┐   │
        └─────────────────────────────────────────────┼───┘
                                                      │
                       UDP: TASK [cloudpickle]        │ (Selects least-loaded
                                                      │  healthy peer)
                                                      ▼
                                       ┌─────────────────────────────┐
                                       │  Worker Node                │
                                       │                             │
                                       │   Router (Type Dispatch)    │
                                       │             │               │
                                       │             ▼               │
                                       │   Executor (ThreadPool)     │
                                       │             │               │
                                       │             ▼               │
                                       │       func(*args)           │
                                       └─────────────┬───────────────┘
                                                     │
                                 UDP: RESULT         │
                                                     ▼
                                              Coordinator Node
                                                     │
                                                     ▼
                                          Client Receives Result
```

### Core Subsystems

| Subsystem | Primary Responsibility | Key Mechanism |
|---|---|---|
| **UDP Network** | Non-blocking packet transport | `asyncio.DatagramProtocol` with Winsock reset recovery |
| **Kademlia DHT** | Decentralized peer discovery | 160-bit node IDs, XOR metric, $k$-buckets, transitive discovery |
| **Gossip Engine** | Metric and topology dissemination | Periodic randomized fan-out of CPU/RAM status |
| **Heartbeat Monitor** | Cluster liveness and failure detection | Sliding-window miss counts, peer dead marking |
| **Task Scheduler** | Load-aware compute distribution | CPU and memory weighted selection with failure exclusion |
| **Fault Tolerance** | Automatic retry and rerouting | Transparent task reassignment upon node crash/timeout |
| **Security Layer** | Frame authentication and validation | Strict envelope schema parsing and HMAC-SHA256 signatures |

---

## Key Features

- **Decentralized P2P Networking**: Direct node-to-node communication over real operating system UDP sockets without centralized brokers.
- **Robust Bootstrap Handshake**: Reliable `HELLO` handshake with exponential retries and configurable timeouts, resilient against concurrent cold starts.
- **Dynamic Peer Discovery**: Transitive peer learning via Kademlia DHT routing tables; introducing Node C to Node B transitively connects Node C to Node A.
- **Telemetry Gossip**: Lightweight background gossip protocol sharing CPU and memory utilization every 5 seconds.
- **Heartbeat Failure Detector**: Unresponsive nodes transition from `ALIVE` to `DEAD` after missing a configurable threshold of heartbeat acknowledgments.
- **Fault-Tolerant Task Rerouting**: If a worker crashes or terminates abruptly mid-execution, the coordinator detects the failure, excludes the dead peer, and reroutes the workload to a healthy standby worker.
- **Interactive Function Serialization**: Uses `cloudpickle` to ship lambda functions, nested closures, and functions defined in `__main__` across the network.
- **Non-Blocking ThreadPool Executor**: CPU-bound tasks execute off the main event loop in a managed thread pool, keeping networking and gossip responsive.
- **Task Lifecycle & Cancellation**: Live tracking across states (`PENDING`, `RUNNING`, `RETRYING`, `COMPLETED`, `FAILED`, `CANCELLED`) with remote task cancellation.
- **Graceful Remote Shutdown**: Remote `node stop` command cleanly tears down background loops and closes network sockets.
- **Structured CLI Arguments**: Supports JSON-encoded positional (`--args-json`) and keyword (`--kwargs-json`) arguments parsed strictly with `json.loads` (no `eval`).
- **Live Terminal Dashboard**: Text-based live monitoring dashboard rendering node status, task counters, peer metrics, and topology trees.

---

## Technology Stack

- **Language**: Python 3.11+
- **Concurrency**: Python Standard Library `asyncio`
- **Networking**: Raw UDP Datagrams via `asyncio.DatagramProtocol`
- **Serialization**: `cloudpickle` (version 3.0+)
- **System Metrics**: `psutil` (with graceful fallback to OS load-averages)
- **Quality & Linting**: `ruff`, `pytest`, `pytest-asyncio`
- **Packaging**: Standard PEP 517 / PEP 621 (`pyproject.toml`)

---

## Installation

### From Source (Editable Mode)

```bash
# Clone repository
cd MeshWeaver

# Create and activate virtual environment
python -m venv .venv

# On Linux / macOS:
source .venv/bin/activate
# On Windows (PowerShell):
.venv\Scripts\Activate.ps1

# Install editable package with development tooling
pip install -e ".[dev]"
```

After installation, the `meshweaver` command-line utility is available globally in the active environment.

---

## Quick Start

### 1. PING/PONG Validation

Verify the local message router with the bundled prototype demo:

```bash
python app.py
```

### 2. Multi-Node Cluster Setup

Open three separate terminals to start a 3-node cluster:

**Terminal 1 — Coordinator Node (Node A):**
```bash
meshweaver node start --node-id node_a --host 127.0.0.1 --port 5000
```

**Terminal 2 — Worker Node 1 (Node B):**
```bash
meshweaver node start --node-id node_b --host 127.0.0.1 --port 5001 --bootstrap 127.0.0.1:5000
```

**Terminal 3 — Worker Node 2 (Node C):**
```bash
meshweaver node start --node-id node_c --host 127.0.0.1 --port 5002 --bootstrap 127.0.0.1:5001
```
*(Notice Node C bootstraps through Node B; Node C will discover Node A transitively via DHT).*

---

## Task Execution & Management

### 1. Explicit Worker Targeting
Dispatch a function directly to a specific worker:

```bash
meshweaver submit \
    --target node_b \
    --target-host 127.0.0.1 \
    --target-port 5001 \
    --function examples.simple_task:add \
    --args 10 32
```

**Output:**
```text
Task ID: 6492fdb848494914a7cec2cfe61c29f5
Target: node_b
Status: COMPLETED
Result: 42
```

### 2. Automatic CPU-Aware Scheduling
Omit `--target` to allow MeshWeaver's scheduler to query known peers, inspect live CPU/RAM utilization, and route to the optimal node:

```bash
meshweaver submit \
    --host 127.0.0.1 \
    --port 5000 \
    --function examples.simple_task:add \
    --args 100 250
```

**Output:**
```text
Task ID: bcc3c9b9fa22416c9d4746b2204a7ba5
Target: node_c
Status: COMPLETED
Result: 350
```

### 3. Structured JSON Arguments
Execute functions requiring complex types (lists, dictionaries, booleans, floats) using `--args-json` and `--kwargs-json`:

```bash
meshweaver submit \
    --host 127.0.0.1 \
    --port 5000 \
    --function examples.complex_task:word_frequency \
    --args-json '["apple orange banana apple banana apple"]'
```

*Windows PowerShell Quoting:*
```powershell
meshweaver submit --host 127.0.0.1 --port 5000 --function examples.complex_task:word_frequency --args-json '["apple banana apple"]'
```

*Windows Command Prompt (CMD) Quoting:*
```cmd
meshweaver submit --host 127.0.0.1 --port 5000 --function examples.complex_task:word_frequency --args-json "[\"apple banana apple\"]"
```

### 4. Query Task Status
Inspect the status and result of any task:

```bash
meshweaver task status --host 127.0.0.1 --port 5000 --task-id 6492fdb848494914a7cec2cfe61c29f5
```

**Output:**
```text
task_id : 6492fdb848494914a7cec2cfe61c29f5
state   : COMPLETED
target  : node_b
result  : 42
error   : None
```

### 5. Remote Task Cancellation
Cancel long-running or queued tasks:

```bash
meshweaver task cancel --host 127.0.0.1 --port 5000 --task-id <TASK_ID>
```

### 6. Graceful Remote Shutdown
Cleanly stop an active node over UDP:

```bash
meshweaver node stop --host 127.0.0.1 --port 5002
```

---

## Live Monitoring Dashboard

Launch the live terminal dashboard to inspect cluster topology, peer telemetry, and task status distributions:

```bash
meshweaver node peers --host 127.0.0.1 --port 5000
# or launch the continuous live view:
python -m meshweaver.dashboard --host 127.0.0.1 --port 5000
```

```text
============================================================
 MESHWEAVER DASHBOARD
============================================================
 Watching node: node_a @ 127.0.0.1:5000
 State: RUNNING
------------------------------------------------------------
 Tasks: total=12 running=1 pending=0 completed=10 failed=1 cancelled=0
------------------------------------------------------------
 Known peers: 2 (2 active, 0 failed)
   - node_b       127.0.0.1:5001   [ALIVE  ] cpu= 12.4% mem= 48.2%
   - node_c       127.0.0.1:5002   [ALIVE  ] cpu=  4.1% mem= 46.8%
------------------------------------------------------------
 Mesh topology:
   node_a
   ├── node_b (ALIVE)
   └── node_c (ALIVE)
============================================================
 Last updated: 21:30:15  (Ctrl+C to exit)
```

---

## Security Model & Honest Limitations

MeshWeaver provides an explicit, transparent security posture designed for distributed systems practitioners:

1. **HMAC-SHA256 Authentication (Not Encryption)**:
   - Message envelopes can be cryptographically signed and verified using a shared secret (`meshweaver/security.py`).
   - HMAC guarantees **authenticity and tamper-resistance** (preventing unauthorized nodes from injecting malicious tasks, cancellations, or node-stop commands).
   - HMAC **does not encrypt traffic**. Transport remains plaintext UDP.
2. **cloudpickle Execution Context (Not a Sandbox)**:
   - `cloudpickle` deserializes executable Python byte code. Running a task executes with the operating system permissions of the node process.
   - Nodes should only accept tasks from authenticated, trusted peers.
3. **Transport Security (TLS / DTLS)**:
   - Python's standard library `ssl` module strictly supports stream-oriented protocols (TCP). Standard library Python **does not support DTLS** (Datagram Transport Layer Security over UDP).
   - Implementing native DTLS would require heavy external C-extensions or OpenSSL wrappers (e.g., `cryptography`, `aioquic`), which violates the zero-heavy-dependency charter.
4. **Recommended Production Deployment**:
   - In production or untrusted networks, MeshWeaver traffic should be routed across an encrypted overlay network (such as **WireGuard**, **IPsec**, or an **authenticated VPC**).

---

## Testing & Verification

MeshWeaver is validated using both an automated unit/integration test suite and a multi-process real-network verification suite:

```bash
# 1. Automated unit & integration tests
python -m pytest -v

# 2. Real multi-process network verification
python verify_all.py
```

### Verified Baseline

- **Automated Tests**: **92 / 92 passed** (100%) in `pytest`.
- **Real-World Checks**: **20 / 20 passed** (100%) in `verify_all.py`.
- **Repeated Stability**: Verified across 3 consecutive runs with 0 failures, 0 leaked processes, and clean socket releases.

See [`docs/testing.md`](docs/testing.md) for full test scenario definitions and execution logs.

---

## Project Structure

```text
MeshWeaver/
├── meshweaver/                 # Core Python Package
│   ├── __init__.py             # Package exports
│   ├── cli.py                  # CLI implementation & console entrypoint
│   ├── config.py               # NodeConfig and environment variable overrides
│   ├── dht.py                  # Kademlia routing table, XOR metric, k-buckets
│   ├── executor.py             # Thread-pool execution and task state tracking
│   ├── gossip.py               # Periodic CPU/RAM anti-entropy gossip
│   ├── heartbeat.py            # Liveness probes and failure detection
│   ├── logging_config.py       # Structured event logging
│   ├── monitor.py              # Telemetry readings (psutil / loadavg fallback)
│   ├── network.py              # Real asyncio UDP transport & Winsock resilience
│   ├── node.py                 # Coordinator, lifecycle state, and RPC handlers
│   ├── peer.py                 # Peer registry and state transitions
│   ├── protocol.py             # Message envelopes, 20 message types, validation
│   ├── router.py               # Message type dispatcher
│   ├── scheduler.py            # Load-aware worker selection and rerouting
│   ├── security.py             # HMAC-SHA256 signing and envelope validation
│   ├── serializer.py           # cloudpickle Task wrapping and serialization
│   └── dashboard.py            # Terminal dashboard interface
│
├── tests/                      # Automated Test Suite (92 tests)
│   ├── test_bootstrap.py       # Bootstrap discovery retry logic
│   ├── test_cli_features.py    # CLI parser and remote commands
│   ├── test_dht.py             # Kademlia XOR routing calculations
│   ├── test_executor.py        # Task execution, timeouts, and cancellations
│   ├── test_gossip.py          # Gossip fan-out convergence
│   ├── test_heartbeat.py       # Heartbeat misses and dead peer detection
│   ├── test_integration.py     # End-to-end task execution and failover
│   ├── test_network.py         # Real asyncio UDP socket exchange
│   ├── test_node.py            # Node lifecycle and PING/PONG
│   ├── test_node_stop.py       # Remote graceful shutdown verification
│   ├── test_peer.py            # Peer status state machines
│   ├── test_protocol.py        # Protocol parsing and bounds checking
│   ├── test_scheduler.py       # CPU/RAM scheduling heuristics
│   ├── test_security.py        # HMAC tampering rejection
│   ├── test_serializer.py     # cloudpickle closures
│   ├── test_structured_args.py # Safe JSON argument deserialization
│   └── test_task_lifecycle.py  # Task state progression
│
├── docs/                       # Technical Specifications & Guides
│   ├── architecture.md         # Component diagrams and dependency flow
│   ├── dht.md                  # Kademlia DHT implementation details
│   ├── failure_tolerance.md    # Heartbeat and rerouting mechanics
│   ├── networking.md           # UDP transport & DTLS feasibility analysis
│   ├── protocol.md             # Wire protocol envelope specifications
│   ├── task_execution.md       # Serialization and execution security
│   └── testing.md              # Two-tier testing methodology & test matrix
│
├── examples/                   # Executable Task Examples
│   ├── simple_task.py          # Arithmetic tasks (add, multiply)
│   ├── complex_task.py         # CPU tasks (word_frequency, primes)
│   └── ml_task.py              # Linear regression & clustering
│
├── pyproject.toml              # PEP 517/621 packaging & tool configs
├── requirements.txt            # Dependency definitions
├── CHANGELOG.md                # Release history and feature notes
├── CONTRIBUTING.md             # Contribution and development guidelines
├── app.py                      # Original Week 1 & 2 PING/PONG demo
└── verify_all.py               # 20-scenario real-network verification suite
```

---

## Design Decisions

1. **UDP Over TCP**: Avoids connection state overhead and head-of-line blocking for high-frequency heartbeats and gossip messages. Request correlation IDs layer reliable request-response semantics atop datagrams.
2. **Zero-Heavy-Dependency Charter**: Keeps deployment trivial. No C-compilers or heavy runtime orchestrators (like Celery, Redis, or RabbitMQ) are required.
3. **Decoupled Transport Interface**: The network layer exposes an abstract `send(message, addr)` and `on_message` interface, permitting alternate transports (such as a stream-based TLS transport) to be swapped in without modifying coordinator or scheduler logic.
4. **Thread Pool Execution**: Keeps Python's single-threaded `asyncio` event loop free of blocking compute, preventing CPU-intensive tasks from delaying heartbeat checks.

---

## Future Improvements

- **TCP/TLS Stream Transport Adapter**: An optional stream-oriented transport plugin for environments requiring transport-layer encryption without VPN tunnels.
- **Dynamic Worker Pool Scaling**: Elastic auto-scaling of worker thread pools based on hardware capabilities and queue depths.
- **Distributed Result Storage**: DHT-based key-value storage for retaining completed task results across multiple nodes.

---

## License

LICENSE is missing and requires the author's choice.
