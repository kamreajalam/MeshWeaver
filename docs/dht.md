# Peer Discovery / DHT

MeshWeaver uses a lightweight, genuine (if intentionally minimal)
Kademlia-style scheme for peer discovery — no hardcoded topology.

## Node IDs

Each node's DHT id is `sha1("host:port") mod 2**id_bits`
(`dht.node_id_for_address`). Deriving the id from the address rather than
generating it randomly at startup means restarting a node at the same
address is reproducible, which is convenient for local demos and tests,
while still giving every distinct address its own point in the id space.

## XOR distance and routing table

`distance(a, b) = a ^ b`. Peers are grouped into `RoutingTable` buckets
indexed by `distance(owner_id, peer_id).bit_length()`, each holding up to
`k` peers (`NodeConfig.dht_k`, default 8) — the standard Kademlia
k-bucket structure. `find_nearest(target_id, count)` returns the closest
known ids by XOR distance.

## How discovery actually happens (no hardcoded topology)

1. A new node starts with zero peers, or a small `--bootstrap` list of
   addresses.
2. It sends `HELLO` to each bootstrap address. `HELLO` is handled
   symmetrically: the receiver records the sender as a peer
   (`peer.PeerManager.add_peer` + `dht.observe_peer`) and replies with its
   own `HELLO`, carrying a short list of *its* known peers.
3. The original sender absorbs that peer list (`Node._absorb_peer_list`),
   discovering peers it never contacted directly.
4. `PEER_REQUEST` / `PEER_RESPONSE` let a node explicitly ask "who else do
   you know?" at any time (used by `discover_peers()`).
5. `GOSSIP` (see `docs/failure_tolerance.md`) keeps already-known peers'
   liveness/load data fresh, but does not itself introduce brand-new
   peers — discovery is HELLO/PEER_REQUEST driven, gossip is status-driven.

This is enough for transitive discovery: if A bootstraps B, and B
bootstraps C, C learns about A through B's peer list without ever being
told about A directly (`tests/test_integration.py::test_three_node_mesh_discovery`).

## What's simplified vs. full Kademlia

This implementation covers what the project specification asks for
(deterministic ids, XOR distance, k-buckets, nearest-node lookup, dynamic
joining) but does not implement the full Kademlia paper:

- No `STORE`/`FIND_VALUE` key-value storage — MeshWeaver doesn't need a
  DHT-backed data store, only peer discovery, so those message type names
  are reserved in the protocol but not wired to a storage backend.
- No parallel alpha-lookup or iterative `FIND_NODE` RPC chase across the
  network — nearest-node lookup (`DHT.find_nearest`) is computed locally
  against whatever peers are already known, and new peers arrive via the
  HELLO/PEER_REQUEST exchange described above rather than a recursive
  network walk.
- No bucket refresh/replacement-cache eviction policy for stale entries
  beyond what `PeerManager`/`HeartbeatManager` already provide (dead peers
  are marked `DEAD`, not actively evicted from the routing table).

These are documented gaps, not silently missing functionality — see the
Definition of Done table in the top-level README for what's COMPLETE vs.
PARTIAL.
