# MeshWeaver Wire Protocol

Every message on the wire is a JSON-encoded `Message` (see
`meshweaver/protocol.py`). Task *payloads* inside a `TASK` message are the
one exception: they're a base64-encoded cloudpickle blob nested inside the
JSON payload dict, so the envelope itself is always inspectable/loggable
without deserializing (and potentially executing) anything.

## Message shape

```json
{
  "type": "PING",
  "sender": "node_a",
  "receiver": "node_b",
  "message_id": "3a43f359fae1439ba9f2a57403750b23",
  "timestamp": 1732000000.123,
  "payload": {},
  "correlation_id": null
}
```

| Field            | Meaning                                                            |
|------------------|---------------------------------------------------------------------|
| `type`           | One of the message types below.                                    |
| `sender`         | node_id of the sender.                                             |
| `receiver`       | node_id of the intended recipient, or `"*"` when the sender doesn't yet know the recipient's node_id (e.g. an initial PING to an address). |
| `message_id`     | Unique id, auto-generated.                                          |
| `timestamp`      | Unix time at construction.                                          |
| `payload`        | Message-specific data (dict).                                       |
| `correlation_id` | Set on a reply to the `message_id` of the message it answers.       |

## Message types

| Type              | Direction        | Purpose                                             |
|-------------------|------------------|------------------------------------------------------|
| `PING` / `PONG`    | request/response | Basic liveness check.                                |
| `HELLO`            | request+response | Greeting exchanged on bootstrap; carries a short peer list both ways for discovery. |
| `PEER_REQUEST` / `PEER_RESPONSE` | req/resp | Explicitly ask a peer for its known-peers list. |
| `GOSSIP`           | fire-and-forget  | Periodic CPU/RAM/state broadcast to a random peer subset. |
| `HEARTBEAT` / `HEARTBEAT_ACK` | req/resp | Liveness probe used for failure detection.        |
| `TASK`             | request          | A serialized function call to execute remotely.      |
| `RESULT`           | response         | Successful task outcome.                             |
| `TASK_FAILED`      | response         | Task raised an exception, timed out, or failed to deserialize. |
| `TASK_ASSIGN` / `TASK_CANCEL` | control  | Task assignment and cancellation request. |
| `TASK_CANCEL_ACK` | response         | Acknowledgement of task cancellation outcome. |
| `NODE_STOP`        | control          | Remote request to gracefully stop a node process. |
| `NODE_STOP_ACK`    | response         | Acknowledgement of remote node shutdown. |
| `STATUS`           | request+response | Remote query of node or task status (used by the CLI/dashboard). |
| `ERROR`            | response         | Generic error reply (e.g. malformed request).        |
| `FIND_NODE` / `FIND_NODE_RESPONSE` / `STORE` / `FIND_VALUE` | — | Reserved DHT operation names; the current DHT implementation performs discovery via `HELLO`/`PEER_REQUEST` peer-list exchange plus local XOR-distance routing rather than issuing these as separate wire messages (see `docs/dht.md`). |

## Request/reply message types are dual-purpose

`HELLO` and `STATUS` are sent both as an initial request *and* as the
reply to one. A node receiving such a message checks whether its
`correlation_id` matches an outstanding local request; if so, it's treated
purely as a reply (resolving the waiting caller) and is **not** replied to
again — otherwise two nodes would volley HELLO/STATUS messages at each
other indefinitely.

## Validation

Before a handler ever sees a message, `meshweaver.security.decode_and_validate`
(called from the network layer) rejects:

- Malformed JSON or non-UTF8 bytes.
- Missing required fields (`type`, `sender`, `receiver`).
- Unsupported `type` values.
- Malformed/dangerous-looking `sender`/`receiver` node ids (only a fixed
  alphanumeric-plus-`-_.` charset is accepted).
- Non-dict payloads, and payloads carrying suspicious dunder keys.

Malformed input is logged and dropped — it never raises into the event
loop or crashes the node.
