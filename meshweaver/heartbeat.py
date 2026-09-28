"""
meshweaver.heartbeat
=======================

Periodic heartbeat + failure detection. Each node periodically sends a
HEARTBEAT to every known peer and expects a HEARTBEAT_ACK within
`heartbeat_timeout`. Missed acks increment a peer's `missed_heartbeats`
counter; once it reaches `failure_threshold`, the peer is marked DEAD and
an `on_peer_failed` callback fires so the scheduler/router can re-route
any tasks that were assigned to it.
"""

from __future__ import annotations

import asyncio
import time
from typing import Callable, Dict, Optional

from meshweaver.logging_config import HEARTBEAT_TIMEOUT, PEER_LOST, get_logger, log_event
from meshweaver.peer import PeerManager

logger = get_logger("meshweaver.heartbeat")

SendHeartbeatFn = Callable[[tuple], "asyncio.Future"]
OnPeerFailed = Callable[[str], None]


class HeartbeatManager:
    def __init__(
        self,
        node_id: str,
        peer_manager: PeerManager,
        send_heartbeat: SendHeartbeatFn,
        interval: float = 2.0,
        timeout: float = 1.0,
        failure_threshold: int = 3,
        on_peer_failed: Optional[OnPeerFailed] = None,
    ):
        self.node_id = node_id
        self.peer_manager = peer_manager
        self._send_heartbeat = send_heartbeat
        self.interval = interval
        self.timeout = timeout
        self.failure_threshold = failure_threshold
        self.on_peer_failed = on_peer_failed

        self._task: Optional[asyncio.Task] = None
        self._running = False
        # node_id -> monotonic timestamp of the most recent ACK received.
        # Timestamp-based rather than a per-round asyncio.Event: an ACK
        # that arrives slightly before its round's wait begins (e.g. under
        # scheduler jitter) would be silently lost by an Event created
        # only once the round starts. Comparing "ack time >= round start
        # time" after a plain sleep is race-free.
        self._last_ack: Dict[str, float] = {}

    def start(self) -> None:
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name=f"heartbeat-{self.node_id}")

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
                await self._heartbeat_round()
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            raise

    async def _heartbeat_round(self) -> None:
        peers = [p for p in self.peer_manager.get_peers() if p.status != "DEAD"]
        await asyncio.gather(*(self._check_peer(p.node_id) for p in peers), return_exceptions=True)

    async def _check_peer(self, node_id: str) -> None:
        peer = self.peer_manager.get_peer(node_id)
        if peer is None:
            return

        round_start = time.monotonic()

        try:
            await self._send_heartbeat(peer.address)
        except Exception as exc:
            logger.warning("Failed to send heartbeat to %s: %s", node_id, exc)

        await asyncio.sleep(self.timeout)

        acked = self._last_ack.get(node_id, 0.0) >= round_start
        if acked:
            peer.touch()
            return

        peer.missed_heartbeats += 1
        log_event(
            logger, HEARTBEAT_TIMEOUT, peer=node_id, missed=peer.missed_heartbeats
        )
        if peer.missed_heartbeats >= self.failure_threshold and peer.status != "DEAD":
            peer.status = "DEAD"
            log_event(logger, PEER_LOST, peer=node_id)
            if self.on_peer_failed:
                self.on_peer_failed(node_id)
        elif peer.status == "ALIVE":
            peer.status = "SUSPECT"

    def handle_ack(self, node_id: str) -> None:
        """Called by the router when a HEARTBEAT_ACK arrives."""
        self._last_ack[node_id] = time.monotonic()
