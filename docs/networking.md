# Networking

MeshWeaver nodes communicate over real `asyncio` UDP sockets — there is no
in-process shortcut. Even two nodes on the same machine talk through the
OS network stack via `asyncio.DatagramProtocol`.

## Transport (`meshweaver/network.py`)

`UDPTransport` wraps `loop.create_datagram_endpoint`:

- `start()` binds the socket and begins receiving.
- `send(message, addr)` encodes a `Message` to bytes and sends it via
  `sendto`.
- Inbound datagrams are decoded and security-validated
  (`security.decode_and_validate`) before the registered `on_message`
  callback ever sees them; malformed packets are logged and dropped.
- `stop()` closes the socket.

## Request/response over a message-oriented transport

UDP has no built-in notion of a "response" to a specific packet, so
MeshWeaver layers request/response semantics on top using
`correlation_id`: `Node._request_with_timeout` creates an
`asyncio.Future` keyed by the outgoing message's `message_id`, sends it,
and awaits either a matching reply (whose `correlation_id` equals that
id) or a timeout. This is used for `PING`, `HELLO`, `PEER_REQUEST`,
`TASK`, `HEARTBEAT`, and `STATUS`.

## Timeouts and retries

- Every outbound request has a configurable timeout
  (`NodeConfig.request_timeout`, or a per-call override such as
  `task_timeout`).
- `Node.submit_task` retries on a different peer (via
  `Scheduler.select_node(exclude=...)`) up to `max_task_retries` times if
  a request times out or the remote executor reports failure.
- `HeartbeatManager` runs its own independent liveness loop and marks a
  peer `DEAD` after `failure_threshold` consecutive missed acks,
  triggering task re-routing for anything still assigned to that peer.

## Graceful shutdown

`Node.stop()`:

1. Stops the gossip and heartbeat background loops (cancels their tasks
   and awaits them).
2. Shuts down the executor's thread pool (`cancel_futures=True`).
3. Closes the UDP transport.

No sockets or background `asyncio.Task`s are left dangling after `stop()`
returns, which the integration tests exercise directly (starting and
stopping multiple nodes per test run without leaking file descriptors).

## Starting nodes from the CLI

```bash
python -m meshweaver.cli node start --node-id node_a --host 127.0.0.1 --port 5000
python -m meshweaver.cli node start --node-id node_b --host 127.0.0.1 --port 5001 --bootstrap 127.0.0.1:5000
```

`--bootstrap` accepts a comma-separated `host:port` list; on startup the
node sends a `HELLO` to each, which is enough to join the mesh and start
discovering further peers transitively (see `docs/dht.md`).

## Transport Security & DTLS Feasibility

### Standard Library Limitations
The Python standard library `ssl` module strictly supports stream-oriented
protocols (TCP / `socket.SOCK_STREAM`). Python's stdlib does **not** provide
bindings for Datagram Transport Layer Security (DTLS over `socket.SOCK_DGRAM` / UDP).

### Zero-Dependency Constraint
To implement genuine DTLS, an external dependency wrapping OpenSSL's DTLS bio
or a QUIC stack (such as `cryptography`, `pyOpenSSL`, or `aioquic`) would be required.
Introducing these C-extensions or external wheels directly violates MeshWeaver's
core requirement of having zero heavy runtime dependencies beyond standard library
and minimal utilities (`cloudpickle`, `psutil`).

### Security Posture & Architecture
1. **No Fake Encryption**: MeshWeaver explicitly rejects implementing homemade
   crypto or claiming that HMAC authentication is encryption.
2. **Message Authentication**: Message envelopes can be cryptographically
   authenticated and tamper-checked using HMAC-SHA256 (`meshweaver/security.py`).
   This guarantees that only authorized nodes with the shared secret can issue
   tasks or control commands (`NODE_STOP`, `TASK_CANCEL`).
3. **Pluggable Architecture**: The `UDPTransport` layer in `meshweaver/network.py`
   is cleanly decoupled behind a uniform `send(message, addr)` and `on_message`
   interface. When deploying MeshWeaver across untrusted public networks, users
   can wrap the transport in an external tunnel (WireGuard, IPsec) or plug in a
   TLS/TCP stream transport without modifying node or scheduler logic.

