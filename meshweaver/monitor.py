"""
meshweaver.monitor
=====================

Local resource monitoring (CPU/RAM), used by gossip and the task
scheduler for load-aware routing.

Uses psutil when available (it's a small, common, pure-metrics dependency
already implied by the project needing real CPU/RAM numbers). Falls back
to a conservative, clearly-labeled estimate via os.getloadavg() when
psutil isn't installed, rather than fabricating precise numbers we can't
actually measure.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:  # pragma: no cover - exercised only without psutil installed
    psutil = None
    _HAS_PSUTIL = False


@dataclass
class ResourceSnapshot:
    cpu_percent: float
    memory_percent: float
    source: str  # "psutil" or "loadavg_estimate"

    def to_dict(self) -> dict:
        return {
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "source": self.source,
        }


def get_cpu_usage() -> float:
    """Return CPU usage as a percentage in [0, 100]."""
    if _HAS_PSUTIL:
        return float(psutil.cpu_percent(interval=None))
    return _loadavg_estimate()


def get_memory_usage() -> float:
    """Return memory usage as a percentage in [0, 100]."""
    if _HAS_PSUTIL:
        return float(psutil.virtual_memory().percent)
    return 0.0  # honestly unknown without psutil; do not fabricate a number


def get_snapshot() -> ResourceSnapshot:
    if _HAS_PSUTIL:
        return ResourceSnapshot(
            cpu_percent=get_cpu_usage(),
            memory_percent=get_memory_usage(),
            source="psutil",
        )
    return ResourceSnapshot(
        cpu_percent=get_cpu_usage(),
        memory_percent=get_memory_usage(),
        source="loadavg_estimate",
    )


def _loadavg_estimate() -> float:
    """Rough CPU-load estimate from the 1-minute load average when psutil
    isn't available. Normalized against CPU count and clamped to [0, 100].
    This is explicitly an estimate, not a precise measurement.
    """
    try:
        load1, _, _ = os.getloadavg()
        cpu_count = os.cpu_count() or 1
        return max(0.0, min(100.0, (load1 / cpu_count) * 100.0))
    except (OSError, AttributeError):
        return 0.0
