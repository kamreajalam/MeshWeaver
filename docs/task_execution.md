# Task Serialization and Remote Execution

## Serialization (`meshweaver/serializer.py`)

Tasks are serialized with **cloudpickle** (as required by the project
spec), not stdlib `pickle`, because cloudpickle can serialize closures,
lambdas, and functions defined in `__main__` or interactively — stdlib
pickle generally cannot.

```python
from meshweaver.serializer import Task

task = Task.create(add, (2, 3), sender="node_a", target="node_b")
```

`Task.create()` immediately serializes `func`/`args`/`kwargs` into
`task.payload` (bytes) and never calls `func` — serialization and
execution are strictly separate steps, enforced by tests
(`test_serialize_does_not_execute`).

A `Task` embeds into a `Message.payload` as base64 text via
`to_message_payload()` / `from_message_payload()`, keeping the outer
protocol envelope plain JSON (see `docs/protocol.md`).

## Execution (`meshweaver/executor.py`)

`Executor` runs deserialized functions in a bounded `ThreadPoolExecutor`
via `loop.run_in_executor`, so a slow or CPU-bound task never blocks the
asyncio event loop that's also handling networking, gossip, and
heartbeats for every other peer.

Task states: `QUEUED -> RUNNING -> {COMPLETED | FAILED | TIMEOUT | CANCELLED}`.

Every failure mode returns a structured result instead of raising:

| Failure                  | Result                                             |
|---------------------------|----------------------------------------------------|
| Function raises            | `{"status": "error", "message": "<exception str>"}` |
| Exceeds `timeout`           | `{"status": "error", "state": "TIMEOUT", ...}`      |
| Malformed/garbage payload  | `{"status": "error", "message": "Deserialization failed: ..."}` |
| Cancelled                   | Task record marked `CANCELLED`                     |

`tests/test_executor.py::test_node_survives_multiple_failed_tasks` proves
repeated crashing tasks never take down the executor.

## End-to-end flow

```python
result = await node_a.submit_task(add, (2, 3), target="node_b")
# {"status": "success", "result": 5}
```

See `docs/architecture.md` for the full message-level sequence diagram.

## Security limitations (read this before trusting a mesh with real data)

**cloudpickle payloads are executable Python objects.** Deserializing a
`TASK` payload and calling the resulting function runs arbitrary code
with the receiving executor's full process privileges — there is no
sandbox. This is inherent to what cloudpickle is (it can serialize
functions, closures, and their captured state), not a bug in MeshWeaver's
implementation of it.

Practical implications:

- **Only run MeshWeaver nodes among machines/processes you already
  trust.** Do not accept bootstrap connections from, or expose a node's
  port to, untrusted networks.
- Set `NodeConfig.require_auth = True` and a shared
  `NodeConfig.shared_secret` (or `MESHWEAVER_REQUIRE_AUTH=1` /
  `MESHWEAVER_SHARED_SECRET=...`) to require an HMAC-SHA256 signature on
  every message before it's acted on (`meshweaver/security.py`). This
  proves the sender knows the shared secret; it is **not** equivalent to
  per-node identity/TLS.
- Traffic is plaintext UDP. TLS/per-node keys are documented as future
  work (see the README's Definition of Done) but not implemented — do not
  run an authenticated-but-unencrypted mesh across an untrusted network
  and assume confidentiality.
- The executor does not sandbox, rate-limit, or resource-cap task code
  beyond a wall-clock `timeout` and `max_task_payload_bytes` on the
  envelope size. A malicious or buggy task can still consume CPU/memory
  on the executing node up to the timeout.

If you need to execute code from parties you don't fully trust, MeshWeaver
as shipped is not the right tool — that would require a real sandbox
(container/VM isolation, syscall filtering, resource cgroups), which is
out of scope for a zero-heavy-dependency stdlib-first broker.
