"""
meshweaver.config
==================

Central place for tunables so nothing is hardcoded deep inside modules.
Values can be overridden via constructor args, environment variables, or
CLI flags (CLI flags win, then env vars, then these defaults).
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    try:
        return float(val) if val is not None else default
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    try:
        return int(val) if val is not None else default
    except ValueError:
        return default


@dataclass
class NodeConfig:
    node_id: str
    host: str = "127.0.0.1"
    port: int = 5000

    # Networking
    recv_buffer_size: int = 65536
    request_timeout: float = _env_float("MESHWEAVER_REQUEST_TIMEOUT", 2.0)
    max_retries: int = _env_int("MESHWEAVER_MAX_RETRIES", 3)
    bootstrap_timeout: float = _env_float("MESHWEAVER_BOOTSTRAP_TIMEOUT", 1.0)
    bootstrap_retries: int = _env_int("MESHWEAVER_BOOTSTRAP_RETRIES", 4)
    bootstrap_retry_interval: float = _env_float("MESHWEAVER_BOOTSTRAP_RETRY_INTERVAL", 0.25)

    # Gossip
    gossip_interval: float = _env_float("MESHWEAVER_GOSSIP_INTERVAL", 5.0)
    gossip_fanout: int = _env_int("MESHWEAVER_GOSSIP_FANOUT", 3)

    # Heartbeat / failure detection
    heartbeat_interval: float = _env_float("MESHWEAVER_HEARTBEAT_INTERVAL", 2.0)
    heartbeat_timeout: float = _env_float("MESHWEAVER_HEARTBEAT_TIMEOUT", 1.0)
    failure_threshold: int = _env_int("MESHWEAVER_FAILURE_THRESHOLD", 3)

    # DHT
    dht_k: int = _env_int("MESHWEAVER_DHT_K", 8)          # bucket size (k)
    dht_id_bits: int = _env_int("MESHWEAVER_DHT_ID_BITS", 64)

    # Task execution
    task_timeout: float = _env_float("MESHWEAVER_TASK_TIMEOUT", 10.0)
    max_task_payload_bytes: int = _env_int(
        "MESHWEAVER_MAX_TASK_PAYLOAD", 4 * 1024 * 1024
    )
    max_task_retries: int = _env_int("MESHWEAVER_MAX_TASK_RETRIES", 2)
    executor_max_workers: int = _env_int("MESHWEAVER_EXECUTOR_WORKERS", 4)

    # Security
    require_auth: bool = os.environ.get("MESHWEAVER_REQUIRE_AUTH", "0") == "1"
    shared_secret: str = os.environ.get("MESHWEAVER_SHARED_SECRET", "")
