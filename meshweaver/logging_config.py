"""
meshweaver.logging_config
==========================

Structured-ish logging setup shared by all nodes. Uses stdlib logging with
a consistent format and a set of named event constants so log lines are
greppable/parseable without pulling in an external logging framework.
"""

from __future__ import annotations

import logging
import sys

# Canonical event names used across the codebase. Keeping them here avoids
# typos scattered through modules and gives one place to see the full
# vocabulary of things MeshWeaver logs.
NODE_STARTED = "NODE_STARTED"
NODE_STOPPED = "NODE_STOPPED"
NODE_FAILED = "NODE_FAILED"
PEER_DISCOVERED = "PEER_DISCOVERED"
PEER_LOST = "PEER_LOST"
PING_SENT = "PING_SENT"
PONG_RECEIVED = "PONG_RECEIVED"
TASK_SUBMITTED = "TASK_SUBMITTED"
TASK_ASSIGNED = "TASK_ASSIGNED"
TASK_STARTED = "TASK_STARTED"
TASK_COMPLETED = "TASK_COMPLETED"
TASK_FAILED = "TASK_FAILED"
TASK_RETRIED = "TASK_RETRIED"
HEARTBEAT_TIMEOUT = "HEARTBEAT_TIMEOUT"
GOSSIP_SENT = "GOSSIP_SENT"
GOSSIP_RECEIVED = "GOSSIP_RECEIVED"

_CONFIGURED = False


def setup_logging(level: int = logging.INFO) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    setup_logging()
    return logging.getLogger(name)


def log_event(logger: logging.Logger, event: str, **fields) -> None:
    """Log a structured event line, e.g.:

        log_event(logger, TASK_COMPLETED, task_id=tid, node=node_id)
        -> "TASK_COMPLETED task_id=... node=..."

    Never logs raw payload/secret fields by name to avoid leaking sensitive
    data; callers should pass only safe, summary-level fields.
    """
    parts = " ".join(f"{k}={v}" for k, v in fields.items())
    logger.info("%s %s", event, parts)
