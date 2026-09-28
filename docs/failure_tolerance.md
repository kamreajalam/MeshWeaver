# Fault Tolerance

## Gossip (`meshweaver/gossip.py`)

Every `gossip_interval` seconds (default 5s, per the spec; configurable
for tests), a node picks up to `gossip_fanout` random ALIVE peers and
sends them a `GOSSIP` message containing its own `node_id`, current CPU%,
memory%, and state. Receivers update the sender's `Peer.cpu`/`.memory`
and `touch()` it (refreshing `last_seen`, clearing `missed_heartbeats`) —
but only if the sender is already a *known* peer; gossip is a liveness/
load signal for peers you've already discovered, not a discovery
mechanism itself (see `docs/dht.md`).

Runs as exactly one cancellable `asyncio.Task` per node — no unbounded
task creation, and `GossipManager.stop()` cancels and awaits it cleanly.

## Heartbeat and failure detection (`meshweaver/heartbeat.py`)

Every `heartbeat_interval` seconds, a node sends `HEARTBEAT` to every
known non-`DEAD` peer and waits up to `heartbeat_timeout` for a
`HEARTBEAT_ACK`.

```
missed ack  -> peer.missed_heartbeats += 1, status -> SUSPECT
missed_heartbeats >= failure_threshold -> status -> DEAD, on_peer_failed(node_id) fires
ack received -> peer.touch() (missed_heartbeats reset to 0, status -> ALIVE)
```

`on_peer_failed` is wired to `Node._on_peer_failed`, which looks up any
task currently assigned to that peer (`Node._outstanding_tasks`) and
triggers `Scheduler.reroute()` for it.

## Task-level fault tolerance (`meshweaver/scheduler.py` + `Node.submit_task`)

Two independent mechanisms cooperate:

1. **Immediate**: `submit_task` itself retries if a `TASK` request times
   out or comes back as `TASK_FAILED`, excluding the failed peer and
   asking the scheduler for the next least-loaded ALIVE candidate, up to
   `max_task_retries`.
2. **Background**: `HeartbeatManager` independently detects a dead peer
   even between task submissions and marks it `DEAD` so *future*
   `select_node()` calls skip it entirely, and reroutes anything already
   outstanding on it.

This means a task submitted to a peer that dies mid-flight is retried on
a different peer without the caller having to do anything —
`tests/test_integration.py::test_task_rerouted_to_available_node_after_failure`
demonstrates this against real UDP sockets and a real stopped node
process (not a mock).

## Selection policy (`Scheduler.select_node`)

Deterministic, greedy least-loaded:

1. Only `ALIVE` peers are eligible.
2. Lowest reported `cpu`, ties broken by lowest `memory`, ties broken by
   `node_id` string order (full determinism for equal conditions, as the
   spec requires).
3. A peer with no metrics yet (`cpu is None`) is treated as load `0.0` so
   a freshly-discovered peer isn't starved before its first gossip round.

## Avoiding infinite retry loops

`max_task_retries` (config, default 2) bounds `Scheduler.reroute()` —
once exhausted, `submit_task` returns
`{"status": "error", "message": "Task failed after exhausting retries"}`
rather than retrying forever.
