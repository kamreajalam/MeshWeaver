"""
meshweaver.executor
======================

Controlled task executor. Evolved from the original blocking-socket
executor.py prototype: this version tracks task state, runs functions in a
thread pool so the asyncio event loop never blocks, supports timeouts and
cancellation, and never lets an exception in one task take down the node.

Security note: executing a deserialized task runs arbitrary Python code
with this process's privileges (see serializer.py / security.py). The
executor assumes the trust decision was already made upstream (by the
Router/security layer) before a task reaches here.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional

from meshweaver.logging_config import TASK_COMPLETED, TASK_FAILED, TASK_STARTED, get_logger, log_event
from meshweaver.serializer import SerializationError, deserialize

logger = get_logger("meshweaver.executor")


class TaskState(str, Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"


@dataclass
class TaskRecord:
    task_id: str
    state: TaskState = TaskState.PENDING
    result: Any = None
    error: Optional[str] = None
    submitted_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    future: Optional["asyncio.Future"] = field(default=None, repr=False)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "state": self.state.value,
            "result": self.result if self.state == TaskState.COMPLETED else None,
            "error": self.error,
            "submitted_at": self.submitted_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


class Executor:
    """Runs tasks off the event loop via a thread pool, tracking state.

    A ThreadPoolExecutor is used (rather than raw threads/processes per
    task) so we get bounded concurrency without spawning unbounded OS
    resources, matching the "avoid unnecessary threads/processes" rule
    from the spec.
    """

    def __init__(self, max_workers: int = 4, default_timeout: float = 10.0):
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="mw-exec")
        self._default_timeout = default_timeout
        self._tasks: Dict[str, TaskRecord] = {}
        self._lock = asyncio.Lock()

    async def submit_task(
        self,
        payload: bytes,
        *,
        task_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> str:
        """Deserialize and schedule a task for execution. Returns the
        task_id immediately; use get_task_status()/get_result() to poll,
        or await execute_task() directly if you want to block on it.
        """
        task_id = task_id or uuid.uuid4().hex
        record = TaskRecord(task_id=task_id, state=TaskState.PENDING)
        async with self._lock:
            self._tasks[task_id] = record

        loop = asyncio.get_running_loop()
        record.future = loop.create_task(
            self._run(task_id, payload, timeout or self._default_timeout)
        )
        return task_id

    async def execute_task(
        self, payload: bytes, *, task_id: Optional[str] = None, timeout: Optional[float] = None
    ) -> dict:
        """Submit a task and await its completion, returning a result dict
        of the same shape used across the codebase:
            {"status": "success", "result": ...}
            {"status": "error", "message": ...}
        """
        tid = await self.submit_task(payload, task_id=task_id, timeout=timeout)
        record = self._tasks[tid]
        try:
            await record.future
        except (asyncio.CancelledError, Exception):
            pass
        return self._to_result_dict(record)

    async def _run(self, task_id: str, payload: bytes, timeout: float) -> None:
        record = self._tasks[task_id]
        record.state = TaskState.RUNNING
        record.started_at = time.time()
        log_event(logger, TASK_STARTED, task_id=task_id)

        loop = asyncio.get_running_loop()

        try:
            func, args, kwargs = deserialize(payload)
        except SerializationError as exc:
            record.state = TaskState.FAILED
            record.error = f"Deserialization failed: {exc}"
            record.finished_at = time.time()
            log_event(logger, TASK_FAILED, task_id=task_id, reason="deserialize")
            return

        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(self._pool, _call, func, args, kwargs),
                timeout=timeout,
            )
            record.result = result
            record.state = TaskState.COMPLETED
            log_event(logger, TASK_COMPLETED, task_id=task_id)

        except asyncio.TimeoutError:
            record.state = TaskState.TIMEOUT
            record.error = f"Task exceeded timeout of {timeout}s"
            log_event(logger, TASK_FAILED, task_id=task_id, reason="timeout")

        except asyncio.CancelledError:
            record.state = TaskState.CANCELLED
            record.error = "Task was cancelled"
            raise

        except Exception as exc:  # the function itself raised
            record.state = TaskState.FAILED
            record.error = str(exc)
            log_event(logger, TASK_FAILED, task_id=task_id, reason="exception")

        finally:
            record.finished_at = time.time()

    def get_task_status(self, task_id: str) -> Optional[TaskState]:
        record = self._tasks.get(task_id)
        return record.state if record else None

    def get_result(self, task_id: str) -> Optional[dict]:
        record = self._tasks.get(task_id)
        if record is None:
            return None
        return self._to_result_dict(record)

    async def cancel_task(self, task_id: str) -> bool:
        record = self._tasks.get(task_id)
        if record is None:
            return False
        if record.state in (TaskState.COMPLETED, TaskState.FAILED, TaskState.TIMEOUT, TaskState.CANCELLED):
            return False
        if record.future is None:
            record.state = TaskState.CANCELLED
            record.error = "Task was cancelled"
            record.finished_at = time.time()
            return True
        record.future.cancel()
        record.state = TaskState.CANCELLED
        record.error = "Task was cancelled"
        record.finished_at = time.time()
        return True

    def get_task_stats(self) -> dict:
        counts = {
            "pending": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
            "total": len(self._tasks),
        }
        for r in self._tasks.values():
            if r.state in (TaskState.PENDING, TaskState.QUEUED):
                counts["pending"] += 1
            elif r.state == TaskState.RUNNING:
                counts["running"] += 1
            elif r.state == TaskState.COMPLETED:
                counts["completed"] += 1
            elif r.state in (TaskState.FAILED, TaskState.TIMEOUT):
                counts["failed"] += 1
            elif r.state == TaskState.CANCELLED:
                counts["cancelled"] += 1
        return counts

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

    @staticmethod
    def _to_result_dict(record: TaskRecord) -> dict:
        if record.state == TaskState.COMPLETED:
            return {"status": "success", "result": record.result}
        return {"status": "error", "message": record.error, "state": record.state.value}


def _call(func: Callable, args: tuple, kwargs: dict) -> Any:
    """Runs in a worker thread — the actual (potentially untrusted) call."""
    return func(*args, **kwargs)
