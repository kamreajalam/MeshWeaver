# MeshWeaver Architecture

```
                    MESHWEAVER
                        │
        ┌───────────────┼────────────────┐
        │               │                │
     NETWORK           DHT             GOSSIP
        │               │                │
     asyncio        Peer Discovery    Monitoring
     UDP             Routing Table    CPU/RAM
        │               │                │
        └───────────────┼────────────────┘
                        │
                     ROUTER
                        │
              ┌─────────┴─────────┐
              │                   │
            TASK                 CONTROL
              │                   │
         SERIALIZER          HEARTBEAT
              │                   │
          EXECUTOR           FAILURE DETECTION
              │                   │
           RESULT             RE-ROUTING
              │
              └──────────────┐
                             │
                          SECURITY
                             │
                          CLI/UI
                             │
                         DASHBOARD
```

## Module map and dependency direction

```
config / protocol / logging_config
            │
         security
            │
         network            (real asyncio UDP)
            │
         peer, dht           (who we know, how we find more)
            │
      serializer, executor   (cloudpickle task lifecycle)
            │
      gossip, heartbeat, scheduler
            │
          router
            │
           node             (ties everything together)
            │
        cli, dashboard
```

Dependencies only flow downward in this list — `node.py` imports everything
above it, but nothing above imports `node.py`. `gossip`, `heartbeat`, and
`scheduler` talk to `node.py` only through plain callback functions
(`send_heartbeat`, `send_gossip`, `on_peer_failed`) passed in at
construction time, not through imports, which is how the "no circular
imports" requirement is satisfied without a monolithic dispatcher.

## Message flow (a single TASK round-trip)

```
Node A                                              Node B
  │  submit_task(add, (2,3))                            │
  │  Task.create() -> cloudpickle payload                │
  │  scheduler.select_node() -> least-loaded ALIVE peer  │
  │  Message(TASK) ──────────── UDP ────────────────────▶│
  │                                                       │  Router.dispatch()
  │                                                       │  security.validate/authenticate
  │                                                       │  Task.from_message_payload()
  │                                                       │  Executor.execute_task()
  │                                                       │    - deserialize (cloudpickle)
  │                                                       │    - run in thread pool
  │                                                       │    - capture result/exception
  │◀──────────────────── UDP ──────── Message(RESULT) ───│
  │  future resolved by correlation_id                    │
  │  returns {"status": "success", "result": 5}          │
```

## Failure detection and re-routing

```
HeartbeatManager (Node A)
  │  every heartbeat_interval: HEARTBEAT -> every known peer
  │  wait heartbeat_timeout for HEARTBEAT_ACK
  │  no ack -> missed_heartbeats += 1 -> peer.status = SUSPECT
  │  missed_heartbeats >= failure_threshold -> peer.status = DEAD
  │                                          -> on_peer_failed(node_id)
  │                                          -> Scheduler.reroute() for any
  │                                             tasks assigned to that peer
  │
submit_task() itself also detects failure independently: if a TASK
request to a peer times out, that peer is excluded and the scheduler
picks the next least-loaded ALIVE peer, up to max_task_retries.
```

## Why UDP as the primary transport

The spec calls for asyncio UDP as primary transport with TCP optional
where reliable transfer is needed. MeshWeaver keeps everything on UDP
(including TASK/RESULT) because:

- The protocol layer enforces a `MAX_MESSAGE_SIZE` (8 MiB) and the
  executor enforces `max_task_payload_bytes`, keeping messages within
  practical UDP payload sizes for a local/LAN demo.
- Every request (`PING`, `TASK`, `PEER_REQUEST`, `HEARTBEAT`, `STATUS`)
  goes through `Node._request_with_timeout`, which already provides
  timeout + retry semantics at the application layer — the same
  reliability TCP would buy us, but uniform across all message types
  instead of only some.

A TCP path can be added later (e.g. `meshweaver.network.TCPTransport`)
for large task payloads without touching the protocol or router layers,
since `Message.to_bytes()/from_bytes()` are transport-agnostic.
