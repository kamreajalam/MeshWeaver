"""
meshweaver.protocol
====================

Defines the wire-level message protocol used by every MeshWeaver node.

All inter-node communication (PING/PONG, task submission, results, peer
discovery, gossip, heartbeats, etc.) is expressed as a `Message`. Messages
are plain dataclasses that serialize to/from JSON-compatible dicts, and
to/from raw bytes for transport over UDP/TCP sockets.

Design notes
------------
- Message *envelopes* (this file) are always JSON. Only task *payloads*
  (function + args/kwargs) use cloudpickle, and only inside the payload
  field of a TASK message (see meshweaver.serializer). This keeps the
  protocol itself inspectable/loggable and avoids executing arbitrary
  bytes just to read a message's type or sender.
- Unknown/malformed messages must never raise uncaught exceptions during
  decode; callers get a `ProtocolError` they can handle safely.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional


# ---------------------------------------------------------------------------
# Message types
# ---------------------------------------------------------------------------

# Core task lifecycle
PING = "PING"
PONG = "PONG"
TASK = "TASK"
RESULT = "RESULT"
ERROR = "ERROR"

# Handshake / peer discovery
HELLO = "HELLO"
PEER_REQUEST = "PEER_REQUEST"
PEER_RESPONSE = "PEER_RESPONSE"

# Gossip / monitoring
GOSSIP = "GOSSIP"

# Heartbeat / failure detection
HEARTBEAT = "HEARTBEAT"
HEARTBEAT_ACK = "HEARTBEAT_ACK"

# Task control
TASK_ASSIGN = "TASK_ASSIGN"
TASK_FAILED = "TASK_FAILED"
TASK_CANCEL = "TASK_CANCEL"
TASK_CANCEL_ACK = "TASK_CANCEL_ACK"
STATUS = "STATUS"

# Node control
NODE_STOP = "NODE_STOP"
NODE_STOP_ACK = "NODE_STOP_ACK"

# DHT (Kademlia-style) operations
FIND_NODE = "FIND_NODE"
FIND_NODE_RESPONSE = "FIND_NODE_RESPONSE"
STORE = "STORE"
FIND_VALUE = "FIND_VALUE"

VALID_MESSAGE_TYPES = {
    PING, PONG, TASK, RESULT, ERROR,
    HELLO, PEER_REQUEST, PEER_RESPONSE,
    GOSSIP,
    HEARTBEAT, HEARTBEAT_ACK,
    TASK_ASSIGN, TASK_FAILED, TASK_CANCEL, TASK_CANCEL_ACK, STATUS,
    NODE_STOP, NODE_STOP_ACK,
    FIND_NODE, FIND_NODE_RESPONSE, STORE, FIND_VALUE,
}

# Sanity limit on wire size for a single message (protocol-level guard,
# independent of any task-size policy in the executor). 8 MiB.
MAX_MESSAGE_SIZE = 8 * 1024 * 1024


class ProtocolError(Exception):
    """Raised when a message cannot be validated, decoded, or encoded."""


def _new_message_id() -> str:
    return uuid.uuid4().hex


@dataclass
class Message:
    """A single MeshWeaver protocol message.

    Attributes:
        type: One of the VALID_MESSAGE_TYPES constants.
        sender: node_id of the sender.
        receiver: node_id of the intended receiver (may be "*" for broadcast
            gossip-style messages).
        message_id: Unique id for this message (auto-generated).
        timestamp: Unix timestamp (float) at construction time.
        payload: Arbitrary JSON-serializable dict of message-specific data.
            For TASK messages, binary (cloudpickle) task data is carried
            base64-encoded inside this dict by meshweaver.serializer.
        correlation_id: Optional id linking a response back to its request
            (e.g. a RESULT's correlation_id equals the originating TASK's
            message_id).
    """

    type: str
    sender: str
    receiver: str
    message_id: str = field(default_factory=_new_message_id)
    timestamp: float = field(default_factory=time.time)
    payload: dict = field(default_factory=dict)
    correlation_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.type not in VALID_MESSAGE_TYPES:
            raise ProtocolError(f"Unsupported message type: {self.type!r}")
        if not self.sender or not isinstance(self.sender, str):
            raise ProtocolError("Message.sender must be a non-empty string")
        if not self.receiver or not isinstance(self.receiver, str):
            raise ProtocolError("Message.receiver must be a non-empty string")
        if self.payload is None:
            self.payload = {}
        if not isinstance(self.payload, dict):
            raise ProtocolError("Message.payload must be a dict")

    # -- serialization ------------------------------------------------

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Message":
        if not isinstance(data, dict):
            raise ProtocolError("Message data must be a dict")

        required = {"type", "sender", "receiver"}
        missing = required - data.keys()
        if missing:
            raise ProtocolError(f"Missing required fields: {sorted(missing)}")

        try:
            return cls(
                type=data["type"],
                sender=data["sender"],
                receiver=data["receiver"],
                message_id=data.get("message_id") or _new_message_id(),
                timestamp=data.get("timestamp", time.time()),
                payload=data.get("payload") or {},
                correlation_id=data.get("correlation_id"),
            )
        except ProtocolError:
            raise
        except Exception as exc:  # defensive: never let a bad dict crash caller
            raise ProtocolError(f"Malformed message data: {exc}") from exc

    def to_bytes(self) -> bytes:
        try:
            data = json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ProtocolError(f"Failed to encode message: {exc}") from exc

        if len(data) > MAX_MESSAGE_SIZE:
            raise ProtocolError(
                f"Encoded message ({len(data)} bytes) exceeds MAX_MESSAGE_SIZE"
            )
        return data

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        if not data:
            raise ProtocolError("Cannot decode an empty message")
        if len(data) > MAX_MESSAGE_SIZE:
            raise ProtocolError(
                f"Received message ({len(data)} bytes) exceeds MAX_MESSAGE_SIZE"
            )
        try:
            parsed = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProtocolError(f"Failed to decode message bytes: {exc}") from exc

        return cls.from_dict(parsed)

    # -- convenience ----------------------------------------------------

    def make_reply(self, type: str, payload: Optional[dict] = None, sender: Optional[str] = None) -> "Message":
        """Build a reply message addressed back to this message's sender,
        automatically carrying the correlation_id.

        `sender` should be given explicitly by the responder whenever the
        original message's `receiver` might be a wildcard ("*", used for
        point-to-point sends where the sender doesn't yet know the
        recipient's node_id, e.g. PING/HELLO/HEARTBEAT). Falls back to
        `self.receiver` for the common case where it's already a real id.
        """
        return Message(
            type=type,
            sender=sender if sender is not None else self.receiver,
            receiver=self.sender,
            payload=payload or {},
            correlation_id=self.message_id,
        )

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return (
            f"Message(type={self.type!r}, sender={self.sender!r}, "
            f"receiver={self.receiver!r}, id={self.message_id[:8]})"
        )
