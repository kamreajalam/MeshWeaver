"""
meshweaver.dht
=================

A genuine, if minimal, Kademlia-style distributed hash table used for peer
discovery. Node IDs are deterministic hashes of "host:port" (so restarting
a node at the same address yields the same id, which is convenient for
local demos and tests), placed into a fixed-size ID space. Peers are
organized into k-buckets by XOR distance and a FIND_NODE-style iterative
lookup is used to discover peers beyond immediate neighbors.

This does not implement the full Kademlia paper (no STORE/republish
expiry, no full parallel alpha-lookup) — it implements the subset the
project specification asks for: deterministic ids, XOR distance, k-buckets,
and nearest-node lookup/discovery, which is enough to bootstrap a real
peer-to-peer mesh without hardcoded topology.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


def node_id_for_address(host: str, port: int, bits: int = 64) -> int:
    """Deterministically derive a node id in [0, 2**bits) from an address.

    Using a hash of the address (rather than a purely random id) means two
    processes that both bind to the same host:port will agree on the same
    DHT id, which keeps local multi-node demos reproducible.
    """
    digest = hashlib.sha1(f"{host}:{port}".encode("utf-8")).digest()
    value = int.from_bytes(digest, "big")
    return value % (2 ** bits)


def distance(a: int, b: int) -> int:
    """XOR distance between two node ids."""
    return a ^ b


@dataclass
class KBucket:
    """Holds up to `k` node ids that share a distance prefix."""

    k: int
    node_ids: List[int] = field(default_factory=list)

    def add(self, node_id: int) -> bool:
        if node_id in self.node_ids:
            self.node_ids.remove(node_id)
            self.node_ids.append(node_id)  # move to most-recently-seen
            return True
        if len(self.node_ids) < self.k:
            self.node_ids.append(node_id)
            return True
        return False  # bucket full; caller may decide to evict/ping oldest

    def remove(self, node_id: int) -> None:
        if node_id in self.node_ids:
            self.node_ids.remove(node_id)

    def __len__(self) -> int:
        return len(self.node_ids)


class RoutingTable:
    """A simplified Kademlia routing table: one bucket per bit-length of
    XOR distance from the owning node, each holding up to `k` peers.
    """

    def __init__(self, owner_id: int, k: int = 8, id_bits: int = 64):
        self.owner_id = owner_id
        self.k = k
        self.id_bits = id_bits
        self.buckets: List[KBucket] = [KBucket(k=k) for _ in range(id_bits + 1)]
        # node_id (int) -> (host, port, mesh_node_id)
        self._address_book: Dict[int, Tuple[str, int, str]] = {}

    def _bucket_index(self, node_id: int) -> int:
        dist = distance(self.owner_id, node_id)
        return dist.bit_length()  # 0 for identical id, up to id_bits

    def add_node(self, node_id: int, host: str, port: int, mesh_node_id: str) -> bool:
        if node_id == self.owner_id:
            return False
        self._address_book[node_id] = (host, port, mesh_node_id)
        idx = self._bucket_index(node_id)
        return self.buckets[idx].add(node_id)

    def remove_node(self, node_id: int) -> None:
        idx = self._bucket_index(node_id)
        self.buckets[idx].remove(node_id)
        self._address_book.pop(node_id, None)

    def get_address(self, node_id: int) -> Optional[Tuple[str, int, str]]:
        return self._address_book.get(node_id)

    def all_known_ids(self) -> List[int]:
        return list(self._address_book.keys())

    def find_nearest(self, target_id: int, count: int) -> List[int]:
        """Return up to `count` known node ids sorted by XOR distance to
        `target_id` (ascending = nearest).
        """
        candidates = self.all_known_ids()
        candidates.sort(key=lambda nid: distance(nid, target_id))
        return candidates[:count]


class DHT:
    """Ties a RoutingTable to the owning node's identity and exposes the
    discovery operations the rest of MeshWeaver needs.
    """

    def __init__(self, mesh_node_id: str, host: str, port: int, k: int = 8, id_bits: int = 64):
        self.mesh_node_id = mesh_node_id
        self.host = host
        self.port = port
        self.id_bits = id_bits
        self.self_dht_id = node_id_for_address(host, port, bits=id_bits)
        self.table = RoutingTable(self.self_dht_id, k=k, id_bits=id_bits)

    def observe_peer(self, mesh_node_id: str, host: str, port: int) -> None:
        """Record that a peer exists (learned via HELLO, gossip, or a
        FIND_NODE response) and slot it into the routing table.
        """
        dht_id = node_id_for_address(host, port, bits=self.id_bits)
        self.table.add_node(dht_id, host, port, mesh_node_id)

    def forget_peer(self, host: str, port: int) -> None:
        dht_id = node_id_for_address(host, port, bits=self.id_bits)
        self.table.remove_node(dht_id)

    def distance_to(self, host: str, port: int) -> int:
        return distance(self.self_dht_id, node_id_for_address(host, port, bits=self.id_bits))

    def find_nearest(self, host: str, port: int, count: int) -> List[Tuple[str, int, str]]:
        """Nearest known peers to the given address, as (host, port, mesh_node_id)."""
        target_id = node_id_for_address(host, port, bits=self.id_bits)
        nearest_ids = self.table.find_nearest(target_id, count)
        results = []
        for nid in nearest_ids:
            addr = self.table.get_address(nid)
            if addr:
                results.append(addr)
        return results

    def known_peers(self) -> List[Tuple[str, int, str]]:
        results = []
        for nid in self.table.all_known_ids():
            addr = self.table.get_address(nid)
            if addr:
                results.append(addr)
        return results
