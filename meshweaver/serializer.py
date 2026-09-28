"""
meshweaver.serializer
=======================

Task serialization using cloudpickle, evolved from the original standalone
serializer.py prototype (kept the (func, args, kwargs) tuple approach and
the payload-size logging, replaced the raw-socket transport with
protocol-level base64 embedding so a Task can travel inside a normal
Message.payload).

IMPORTANT — security note (see also security.py / README):
cloudpickle can represent arbitrary executable Python objects. Serializing
a function does not execute it, but *deserializing and then calling it on
the receiving end runs arbitrary code*. Only deserialize/execute tasks
received from nodes you authenticate and trust (see NodeConfig.require_auth
and security.authenticate). This module never executes anything itself —
that happens in executor.py — but it is the boundary where trust decisions
must be enforced upstream.
"""

from __future__ import annotations

import base64
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import cloudpickle

from meshweaver.logging_config import get_logger

logger = get_logger("meshweaver.serializer")


class SerializationError(Exception):
    """Raised when a task fails to serialize or deserialize."""


@dataclass
class Task:
    """A unit of remotely-executable work.

    `func`/`args`/`kwargs` are only populated after deserialization; on the
    sending side, use Task.create() which immediately serializes them into
    `payload` and drops the live function reference (we never want to
    accidentally execute it locally).
    """

    task_id: str
    sender: str
    target: str
    payload: bytes  # cloudpickle blob of (func, args, kwargs)
    metadata: dict = field(default_factory=dict)
    timeout: Optional[float] = None
    created_at: float = field(default_factory=time.time)

    @classmethod
    def create(
        cls,
        func: Callable,
        args: tuple = (),
        kwargs: Optional[dict] = None,
        *,
        sender: str,
        target: str,
        timeout: Optional[float] = None,
        metadata: Optional[dict] = None,
    ) -> "Task":
        """Build a Task by serializing `func` with its arguments. Does NOT
        execute `func`.
        """
        kwargs = kwargs or {}
        payload = serialize(func, args, kwargs)
        return cls(
            task_id=uuid.uuid4().hex,
            sender=sender,
            target=target,
            payload=payload,
            metadata=metadata or {},
            timeout=timeout,
        )

    def deserialize_call(self) -> tuple[Callable, tuple, dict]:
        """Deserialize this task's payload back into (func, args, kwargs).
        Does NOT call the function.
        """
        return deserialize(self.payload)

    # -- transport helpers: embed/extract a Task inside a Message.payload --

    def to_message_payload(self) -> dict:
        return {
            "task_id": self.task_id,
            "sender": self.sender,
            "target": self.target,
            "data": base64.b64encode(self.payload).decode("ascii"),
            "metadata": self.metadata,
            "timeout": self.timeout,
            "created_at": self.created_at,
        }

    @classmethod
    def from_message_payload(cls, data: dict) -> "Task":
        try:
            return cls(
                task_id=data["task_id"],
                sender=data["sender"],
                target=data["target"],
                payload=base64.b64decode(data["data"]),
                metadata=data.get("metadata") or {},
                timeout=data.get("timeout"),
                created_at=data.get("created_at", time.time()),
            )
        except (KeyError, ValueError) as exc:
            raise SerializationError(f"Malformed task payload: {exc}") from exc


def serialize(func: Callable, args: tuple = (), kwargs: Optional[dict] = None) -> bytes:
    """Serialize a function and its arguments into bytes. Does not execute
    `func`.
    """
    kwargs = kwargs or {}
    try:
        data = cloudpickle.dumps((func, args, kwargs))
    except Exception as exc:
        raise SerializationError(f"Failed to serialize task: {exc}") from exc

    logger.debug("Serialized task payload: %s bytes", len(data))
    return data


def deserialize(data: bytes) -> tuple[Callable, tuple, dict]:
    """Deserialize bytes back into (func, args, kwargs). Does not call
    the function.
    """
    try:
        func, args, kwargs = cloudpickle.loads(data)
    except Exception as exc:
        raise SerializationError(f"Failed to deserialize task: {exc}") from exc

    return func, args, kwargs


def serialize_result(result: Any) -> bytes:
    try:
        return cloudpickle.dumps(result)
    except Exception as exc:
        raise SerializationError(f"Failed to serialize result: {exc}") from exc


def deserialize_result(data: bytes) -> Any:
    try:
        return cloudpickle.loads(data)
    except Exception as exc:
        raise SerializationError(f"Failed to deserialize result: {exc}") from exc
