"""
meshweaver.peer
==================

Peer records and a PeerManager tracking everything a node knows about
its neighbors: address, liveness, and last-reported resource usage.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Peer:
    node_id: str
    host: str
    port: int
    last_seen: float = field(default_factory=time.time)
    status: str = "UNKNOWN"          # UNKNOWN | ALIVE | SUSPECT | DEAD
    cpu: Optional[float] = None
    memory: Optional[float] = None
    metadata: dict = field(default_factory=dict)
    missed_heartbeats: int = 0

    @property
    def address(self) -> tuple:
        return (self.host, self.port)

    def touch(self) -> None:
        self.last_seen = time.time()
        self.missed_heartbeats = 0
        if self.status != "ALIVE":
            self.status = "ALIVE"

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "last_seen": self.last_seen,
            "status": self.status,
            "cpu": self.cpu,
            "memory": self.memory,
            "metadata": self.metadata,
        }


class PeerManager:
    """Keeps the set of known peers, deduplicated by node_id."""

    def __init__(self):
        self._peers: Dict[str, Peer] = {}

    def add_peer(self, node_id: str, host: str, port: int, **kwargs) -> Peer:
        status = kwargs.pop("status", "ALIVE")
        existing = self._peers.get(node_id)
        if existing:
            existing.host = host
            existing.port = port
            existing.status = status
            existing.touch()
            for k, v in kwargs.items():
                setattr(existing, k, v)
            return existing

        peer = Peer(node_id=node_id, host=host, port=port, status=status, **kwargs)
        self._peers[node_id] = peer
        return peer

    def remove_peer(self, node_id: str) -> bool:
        return self._peers.pop(node_id, None) is not None

    def get_peer(self, node_id: str) -> Optional[Peer]:
        return self._peers.get(node_id)

    def get_peers(self, *, alive_only: bool = False) -> List[Peer]:
        peers = list(self._peers.values())
        if alive_only:
            peers = [p for p in peers if p.status != "DEAD"]
        return peers

    def update_status(self, node_id: str, status: str) -> None:
        peer = self._peers.get(node_id)
        if peer:
            peer.status = status

    def update_metrics(self, node_id: str, *, cpu: Optional[float] = None, memory: Optional[float] = None) -> None:
        peer = self._peers.get(node_id)
        if peer:
            if cpu is not None:
                peer.cpu = cpu
            if memory is not None:
                peer.memory = memory

    def get_nearest_peers(self, target_distance_fn, count: int) -> List[Peer]:
        """Return up to `count` peers ordered by ascending distance, where
        `target_distance_fn(peer.node_id) -> int` is typically the DHT's
        XOR-distance function. Kept generic here to avoid a circular import
        with dht.py.
        """
        alive = self.get_peers(alive_only=True)
        return sorted(alive, key=lambda p: target_distance_fn(p.node_id))[:count]

    def __len__(self) -> int:
        return len(self._peers)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._peers
