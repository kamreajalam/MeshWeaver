from dataclasses import dataclass, field
from typing import Any
import uuid


PING = "PING"
PONG = "PONG"
TASK = "TASK"
RESULT = "RESULT"
ERROR = "ERROR"

@dataclass
class Message:
    """
    Standard message format used by MeshWeaver nodes.
    """

    type: str
    sender: str
    receiver: str | None = None
    payload: Any = None
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict:
        """Convert the message into a dictionary."""
        return {
            "type": self.type,
            "sender": self.sender,
            "receiver": self.receiver,
            "message_id": self.message_id,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Message":
        """Create a Message object from a dictionary."""
        return cls(
            type=data["type"],
            sender=data["sender"],
            receiver=data.get("receiver"),
            message_id=data.get("message_id", str(uuid.uuid4())),
            payload=data.get("payload"),
        )