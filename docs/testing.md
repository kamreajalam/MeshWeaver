# MeshWeaver Testing & Verification Guide

MeshWeaver employs a rigorous two-tier testing methodology to validate its zero-dependency asynchronous distributed architecture:

1. **Automated Unit & Integration Test Suite** (`pytest`): Fast, automated tests validating contracts, state machines, protocol envelopes, and edge cases.
2. **Real Multi-Process & Network Verification Suite** (`verify_all.py`): Live end-to-end distributed system tests running separate OS processes communicating over real UDP sockets with actual process terminations.

---

## 1. Automated Test Suite (pytest)

### Results: 92 passed, 0 failed (100%)

The automated suite is executed with:

```bash
python -m pytest -v
```

### Test Coverage Breakdown

| Test Module | Tests | Focus Area |
|---|---|---|
| `tests/test_protocol.py` | 13 | Message envelope schema, serialization, payload validation, and message size bounds. |
| `tests/test_network.py` | 5 | Real `asyncio.DatagramProtocol` socket binding, packet dispatch, and error handling. |
| `tests/test_serializer.py` | 5 | `cloudpickle` function/closure serialization without pre-execution. |
| `tests/test_executor.py` | 11 | Thread-pool task execution, cancellation, timeouts, and exception resilience. |
| `tests/test_dht.py` | 8 | Kademlia XOR-distance metric, 160-bit node IDs, k-bucket splits, and replacement cache. |
| `tests/test_peer.py` | 5 | Peer state transitions (`ALIVE`, `SUSPECT`, `DEAD`), metric storage, and filtering. |
| `tests/test_gossip.py` | 4 | Periodic randomized peer status broadcast and CPU/RAM gossip convergence. |
| `tests/test_heartbeat.py` | 4 | Heartbeat probe scheduling, missed acknowledgment tracking, and failure declaration. |
| `tests/test_scheduler.py` | 5 | Load-aware worker selection (CPU/RAM-weighted) and node exclusion filtering. |
| `tests/test_security.py` | 6 | HMAC-SHA256 signature verification, timing attack mitigation, and tampering detection. |
| `tests/test_node.py` | 5 | High-level `Node` lifecycle, PING/PONG exchange, and bootstrap discovery. |
| `tests/test_integration.py` | 4 | End-to-end task dispatch, mid-task failure detection, and automatic rerouting. |
| `tests/test_bootstrap.py` | 3 | Bootstrap discovery retries, timeouts, and near-simultaneous node startup. |
| `tests/test_task_lifecycle.py` | 5 | Task status transitions (`PENDING -> RUNNING -> COMPLETED / FAILED / CANCELLED`). |
| `tests/test_node_stop.py` | 3 | Remote graceful shutdown via `NODE_STOP` and authentication validation. |
| `tests/test_structured_args.py` | 3 | `--args-json` and `--kwargs-json` JSON parsing, nested structures, and `eval()` rejection. |
| `tests/test_cli_features.py` | 3 | CLI parser, automatic scheduler selection, and remote status/cancel commands. |

---

## 2. Real Multi-Process Verification (`verify_all.py`)

### Results: 20 passed, 0 failed (100%)

The real verification suite executes genuine OS-level interactions without mocks or simulated networking:

```bash
python verify_all.py
```

### Verification Criteria

- **Separate OS Processes**: Nodes and workers run in independent Python interpreter processes via `subprocess.Popen`.
- **Real UDP Sockets**: Messages travel across the local loopback network interface via OS-level UDP sockets.
- **Real Process Termination**: Node failures are induced by issuing direct OS `kill()` calls (`SIGKILL` / `TerminateProcess`), simulating sudden hardware/process crashes.
- **Winsock & Proactor Resilience**: Verified on Windows with `asyncio`'s Proactor event loop to ensure `WSAECONNRESET` (`WinError 10054`) does not break UDP socket reading loops.
- **Port Cleanup & Leak Prevention**: All child processes and UDP sockets are guaranteed closed and reaped in `finally` blocks.

### The 20 Real-World Verification Checks

| # | Check Name | Verification Scenario | Result |
|---|---|---|:---:|
| 1 | `2-node UDP` | Two independent node processes communicate over OS UDP sockets. | **PASS** |
| 2 | `PING/PONG` | PING dispatched, remote PONG returned with matching `correlation_id`. | **PASS** |
| 3 | `Remote task` | Function serialized with cloudpickle, executed on remote worker, result returned. | **PASS** |
| 4 | `3-node discovery` | Node C bootstraps through Node B and discovers Node A transitively via DHT. | **PASS** |
| 5 | `Gossip` | Nodes periodically exchange CPU and memory metrics over gossip protocol. | **PASS** |
| 6 | `CPU monitoring` | Live OS CPU utilization measured via psutil (with fallback estimation). | **PASS** |
| 7 | `RAM monitoring` | Live OS memory utilization measured via psutil. | **PASS** |
| 8 | `Heartbeat` | Periodic heartbeat probes exchanged and acknowledged between active nodes. | **PASS** |
| 9 | `Failure detection` | Worker stopped; heartbeat acknowledges miss threshold and marks node `DEAD`. | **PASS** |
| 10 | `Scheduler` | Least-loaded node automatically selected based on CPU and memory metrics. | **PASS** |
| 11 | `CLI automatic scheduling` | `submit` run without `--target`; coordinator queries load and picks worker. | **PASS** |
| 12 | `Task rerouting` | Worker process killed during task; coordinator detects crash and reroutes to standby worker. | **PASS** |
| 13 | `Task status` | CLI queries remote task state, verifying genuine lifecycle (`COMPLETED`). | **PASS** |
| 14 | `Task cancellation` | CLI sends `task cancel`; running task on remote worker is stopped safely. | **PASS** |
| 15 | `Node stop` | CLI issues `node stop`; remote node gracefully halts transport and terminates. | **PASS** |
| 16 | `Structured arguments` | Positional and keyword JSON arguments parsed without `eval()` and executed. | **PASS** |
| 17 | `Security` | Structural validation rejects invalid types, malicious keys, and malformed frames. | **PASS** |
| 18 | `HMAC` | HMAC-SHA256 signature verified; tampered messages rejected immediately. | **PASS** |
| 19 | `Dashboard` | Real-time text dashboard renders tasks, active peers, failed peers, and topology. | **PASS** |
| 20 | `app.py` | Legacy Week 1 & 2 PING/PONG demo executes cleanly with exit code 0. | **PASS** |

---

## 3. Repeated Verification Stability

To prove stability against race conditions, port re-use conflicts, and socket binding errors, `verify_all.py` was executed across **3 consecutive full runs**:

- **Run 1**: 20/20 PASS
- **Run 2**: 20/20 PASS
- **Run 3**: 20/20 PASS
- **Stale Processes**: NONE remaining after test execution.
- **Port Cleanup**: PASS (all ephemeral and test ports released).
