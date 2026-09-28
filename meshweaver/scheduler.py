"""
meshweaver.scheduler
=======================

Selects which peer should run a given task, and tracks in-flight task
assignments so failed peers can have their tasks re-routed.

Selection policy (documented, deterministic):
    1. Only ALIVE peers are eligible.
    2. Among eligible peers, pick the one with the lowest reported CPU
       usage (ties broken by lowest memory usage, then by node_id string
       order for full determinism).
    3. If no peer has reported metrics yet (cpu is None), treat unknown
       load as 0.0 so a freshly-discovered peer isn't starved just because
       gossip hasn't reached it yet.

This is intentionally simple (a greedy least-loaded policy) rather than a
weighted scoring system, so its behavior is easy to reason about and test.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from meshweaver.peer import Peer, PeerManager


class NoAvailableNodeError(Exception):
    """Raised when select_node() has no eligible peer to choose from."""


@dataclass
class TaskAssignment:
    task_id: str
    target: str
    attempts: int = 1
    exhausted_targets: List[str] = field(default_factory=list)


class Scheduler:
    def __init__(self, peer_manager: PeerManager, max_retries: int = 2):
        self.peer_manager = peer_manager
        self.max_retries = max_retries
        self._assignments: Dict[str, TaskAssignment] = {}

    def select_node(self, exclude: Optional[List[str]] = None) -> Peer:
        exclude = set(exclude or [])
        candidates = [
            p for p in self.peer_manager.get_peers(alive_only=True)
            if p.status == "ALIVE" and p.node_id not in exclude
        ]
        if not candidates:
            raise NoAvailableNodeError("No available peer to schedule task on")

        candidates.sort(key=lambda p: (p.cpu if p.cpu is not None else 0.0,
                                        p.memory if p.memory is not None else 0.0,
                                        p.node_id))
        return candidates[0]

    def assign(self, task_id: str) -> Peer:
        peer = self.select_node()
        self._assignments[task_id] = TaskAssignment(task_id=task_id, target=peer.node_id)
        return peer

    def reroute(self, task_id: str) -> Optional[Peer]:
        """Attempt to pick a new target for a task whose current target
        failed. Returns None (and marks the task exhausted) once
        max_retries is reached.
        """
        assignment = self._assignments.get(task_id)
        if assignment is None:
            # Unknown task; treat as a first assignment.
            try:
                peer = self.select_node()
            except NoAvailableNodeError:
                return None
            self._assignments[task_id] = TaskAssignment(task_id=task_id, target=peer.node_id)
            return peer

        if assignment.attempts > self.max_retries:
            return None

        assignment.exhausted_targets.append(assignment.target)
        try:
            peer = self.select_node(exclude=assignment.exhausted_targets)
        except NoAvailableNodeError:
            return None

        assignment.target = peer.node_id
        assignment.attempts += 1
        return peer

    def get_assignment(self, task_id: str) -> Optional[TaskAssignment]:
        return self._assignments.get(task_id)
