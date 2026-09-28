# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0]

### Added

#### Core Networking & Protocol
- **Asynchronous UDP Transport**: Non-blocking network layer utilizing `asyncio.DatagramProtocol` with explicit packet validation and error resilience.
- **Windows Socket Handling**: Handled `WSAECONNRESET` (`WinError 10054`) on Windows to ensure datagram read loops are re-armed after remote peer termination.
- **Wire Protocol**: Standardized JSON message envelope featuring 20 message types, unique message identifiers, timestamps, target addressing, and request correlation.

#### Distributed Coordination & Discovery
- **Kademlia DHT Peer Discovery**: 160-bit SHA-1 node identifiers, XOR distance metric calculation, k-bucket routing table, and replacement caches.
- **Reliable Bootstrap Handshake**: Retry-backed `HELLO` handshake with configurable retry count, interval, and timeout to eliminate startup race conditions.
- **Transitive Peer Discovery**: Bidirectional peer list absorption allowing newly joined nodes to discover entire clusters through a single bootstrap peer.
- **Anti-Entropy Gossip**: Periodic randomized gossip distributing CPU and RAM metrics across the mesh.
- **Heartbeat & Failure Detection**: Configurable heartbeat probes and missed ack threshold detection marking unresponsive nodes as `DEAD`.

#### Task Execution & Scheduling
- **cloudpickle Serialization**: Capability to serialize closures, interactive functions, and arbitrary Python callables without executing them during serialization.
- **Thread Pool Execution**: Non-blocking task execution in isolated thread pools preventing event-loop starvation.
- **Load-Aware Scheduler**: CPU and memory weighted worker selection algorithm routing tasks to the least-burdened active peer.
- **Fault-Tolerant Task Rerouting**: Automatic timeout and error detection on worker failure, rescheduling tasks to healthy nodes up to `max_task_retries`.
- **Task Lifecycle Tracking**: Genuine lifecycle states (`PENDING`, `RUNNING`, `RETRYING`, `COMPLETED`, `FAILED`, `CANCELLED`).
- **Remote Task Cancellation**: Protocol and executor support for cancelling pending and executing tasks across the network.

#### Management & CLI
- **Console Script**: Standard `meshweaver` entry point and backward-compatible `python -m meshweaver.cli` invocation.
- **Automatic Scheduling CLI**: Running `meshweaver submit` without `--target` queries the coordinator and automatically dispatches to the optimal worker.
- **Structured JSON Arguments**: Support for `--args-json` and `--kwargs-json` parsed safely with `json.loads` (no `eval`).
- **Remote Lifecycle Commands**: CLI commands for querying task status (`meshweaver task status`), cancelling tasks (`meshweaver task cancel`), and gracefully stopping nodes (`meshweaver node stop`).
- **Terminal Dashboard**: Live monitoring interface displaying node state, active peers, failed peers, CPU/RAM utilization, task metrics breakdown, and topology tree.

#### Security & Quality
- **Envelope Validation**: Strict structural checks filtering invalid types, malformed identifiers, and unauthorized dunder payload keys.
- **HMAC Authentication**: Optional HMAC-SHA256 signature generation and constant-time verification for message authenticity.
- **Packaging & Tooling**: `pyproject.toml` specification for editable installation (`pip install -e .`) and Ruff / pytest configuration.
- **Verification Suite**: 92 automated unit and integration tests, alongside a 20-scenario real-world multi-process network verification suite (`verify_all.py`).
