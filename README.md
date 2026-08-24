# MeshWeaver

A minimal, zero-dependency implementation of a **Kademlia-style Distributed Hash Table (DHT)** in pure Python. MeshWeaver simulates how peer-to-peer networks organize nodes into routing tables, measure distance between peers, and locate the closest nodes to a target ID.

## Features

- **Node abstraction** — every peer has a unique numeric ID
- **K-Buckets** — fixed-size peer lists (`k = 3`) that hold known contacts
- **Routing table** — each node keeps track of its peers through its bucket
- **PING / PONG protocol** — basic liveness check between two nodes
- **XOR distance metric** — used to rank how "close" two node IDs are
- **FIND_NODE lookup** — given a target ID, return the closest known peers
- **Zero dependencies** — runs on the Python standard library only

## Project Structure

```
MeshWeaver/
├── main.py           # Demo: creates nodes, exchanges pings, runs lookups
├── node.py           # Node class with ID and routing table
├── bucket.py         # KBucket: bounded list of peers with closest-first sorting
├── routing_table.py  # RoutingTable: wraps a k-bucket for peer management
├── distance.py       # Distance metric between node IDs
├── find_node.py      # Convenience wrapper for node lookups
├── protocol.py       # PING / PONG message exchange
└── README.md
```

## How It Works

1. **Node creation** — `main.py` spins up 10 nodes with IDs `10, 20, ..., 100`.
2. **Peer discovery** — the first node registers all others in its routing table.
3. **PING/PONG** — node A pings each peer; each peer replies with a PONG containing its ID.
4. **Distance calculation** — the distance between node IDs is computed for every pair.
5. **FIND_NODE** — for each target ID, the routing table returns the peers sorted by distance (closest first).

### Core Concepts

| Concept | Where | Description |
|---|---|---|
| `Node` | `node.py` | A peer identified by `node_id`, holding a `RoutingTable` |
| `KBucket` | `bucket.py` | Holds up to `k` peers; rejects new peers when full |
| `RoutingTable` | `routing_table.py` | Manages the bucket: add peers, query closest |
| `xor_distance(a, b)` | `distance.py` | Metric used to compare node IDs |
| `ping` / `pong` | `protocol.py` | Request/response liveness messages |

## Getting Started

**Requirements:** Python 3.x — no external packages needed.

Clone the repository and run the demo:

```bash
git clone https://github.com/kamreajalam/MeshWeaver.git
cd MeshWeaver
python main.py
```

### Example Usage

```python
from node import Node
from protocol import ping

# Create two nodes
node_a = Node(10)
node_b = Node(20)

# Register node_b in node_a's routing table
node_a.routing_table.add_peer(node_b)

# Ping node_b
response = ping(node_a, node_b)
print(response)  # {'type': 'PONG', 'node_id': 20}

# Find the closest peers to a target ID
closest = node_a.find_node(20)
```

### Sample Output

```
--- NODES ---
Node(10)
Node(20)
...

--- PING / PONG ---
Node 10 -> PING -> Node 20
Node 20 -> PONG -> Node 10
Response: {'type': 'PONG', 'node_id': 20}

--- FIND_NODE ---
Target ID: 20
Closest nodes:
Node ID: 20, ...
```

## Roadmap

- [ ] Implement true XOR-based distance metric
- [ ] Support multiple k-buckets keyed by bit-prefix (full Kademlia routing)
- [ ] UDP transport layer for real network communication
- [ ] STORE / FIND_VALUE RPCs to turn this into a working DHT
- [ ] Peer eviction and refresh policies for stale buckets

## Contributing

Contributions are welcome! Open an issue or submit a pull request.

## License

This project is open source — see the repository for license details.
