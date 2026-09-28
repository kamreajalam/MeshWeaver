"""
meshweaver.gossip
====================

Periodic gossip: each node shares its own status (CPU/RAM/state) with a
random subset of known peers at a configurable interval, and updates its
PeerManager when it hears gossip from others. Runs as a single cancellable
asyncio task per node — no unbounded task creation.
"""

from __future__ import annotations

import asyncio
import random
from typing import Callable, List, Optional

from meshweaver.logging_config import GOSSIP_RECEIVED, GOSSIP_SENT, get_logger, log_event
from meshweaver.monitor import get_snapshot
from meshweaver.peer import PeerManager

logger = get_logger("meshweaver.gossip")

SendFn = Callable[[dict, tuple], "asyncio.Future"]


class GossipManager:
    def __init__(
        self,
        node_id: str,
        peer_manager: PeerManager,
        send_gossip: SendFn,
        interval: float = 5.0,
        fanout: int = 3,
    ):
        self.node_id = node_id
        self.peer_manager = peer_manager
        self._send_gossip = send_gossip
        self.interval = interval
        self.fanout = fanout
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self) -> None:
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name=f"gossip-{self.node_id}")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        try:
            while self._running:
                await self._gossip_once()
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            raise

    async def _gossip_once(self) -> None:
        targets = self._select_targets()
        if not targets:
            return

        snapshot = get_snapshot()
        status = {
            "node_id": self.node_id,
            "cpu": snapshot.cpu_percent,
            "memory": snapshot.memory_percent,
            "state": "RUNNING",
        }

        for peer in targets:
            try:
                await self._send_gossip(status, peer.address)
                log_event(logger, GOSSIP_SENT, to=peer.node_id)
            except Exception as exc:
                logger.warning("Gossip send to %s failed: %s", peer.node_id, exc)

    def _select_targets(self) -> List:
        alive = self.peer_manager.get_peers(alive_only=True)
        if len(alive) <= self.fanout:
            return alive
        return random.sample(alive, self.fanout)

    def handle_gossip(self, payload: dict) -> None:
        """Called by the router when a GOSSIP message arrives."""
        node_id = payload.get("node_id")
        if not node_id or node_id == self.node_id:
            return

        peer = self.peer_manager.get_peer(node_id)
        if peer is None:
            return  # unknown peer; discovery happens via HELLO/DHT, not gossip content alone

        peer.touch()
        self.peer_manager.update_metrics(node_id, cpu=payload.get("cpu"), memory=payload.get("memory"))
        log_event(logger, GOSSIP_RECEIVED, frm=node_id)
