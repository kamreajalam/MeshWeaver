from typing import Callable

from serializer import send_task

from .protocol import (
    ERROR,
    PING,
    PONG,
    RESULT,
    TASK,
    Message,
)
from .security import validate_message


class Router:
    """Routes MeshWeaver messages to the appropriate handler."""

    def __init__(self, node_id: str):
        self.node_id = node_id

        self.handlers: dict[str, Callable[[Message], Message | None]] = {
            PING: self.handle_ping,
            TASK: self.handle_task,
            RESULT: self.handle_result,
            ERROR: self.handle_error,
        }

    def route(self, message: Message) -> Message | None:
        """Validate and route an incoming message."""
        if not validate_message(message):
            return self.handle_unknown(message)

        handler = self.handlers.get(message.type)

        if handler is None:
            return self.handle_unknown(message)

        return handler(message)

    def handle_ping(self, message: Message) -> Message:
        """Respond to PING with PONG."""
        return Message(
            type=PONG,
            sender=self.node_id,
            receiver=message.sender,
            payload={"message": "PONG"},
        )

    def handle_task(self, message: Message) -> Message:
        """Send a trusted TASK payload to the local executor."""
        payload = message.payload if isinstance(message.payload, dict) else {}
        func = payload.get("function")
        args = payload.get("args", ())

        if not callable(func) or not isinstance(args, (list, tuple)):
            return Message(
                type=ERROR,
                sender=self.node_id,
                receiver=message.sender,
                payload={
                    "error": (
                        "TASK payload requires a callable 'function' "
                        "and list/tuple 'args'"
                    )
                },
            )

        response = send_task(func, args)

        if response is None:
            return Message(
                type=ERROR,
                sender=self.node_id,
                receiver=message.sender,
                payload={"error": "Executor is unavailable"},
            )

        if response.get("status") == "error":
            return Message(
                type=ERROR,
                sender=self.node_id,
                receiver=message.sender,
                payload={
                    "error": response.get("message", "Task execution failed")
                },
            )

        return Message(
            type=RESULT,
            sender=self.node_id,
            receiver=message.sender,
            payload={"result": response.get("result")},
        )

    def handle_result(self, message: Message) -> Message:
        """Handle a task result."""
        return message

    def handle_error(self, message: Message) -> Message:
        """Handle an error message."""
        return message

    def handle_unknown(self, message: Message) -> Message:
        """Handle an unsupported or invalid message."""
        return Message(
            type=ERROR,
            sender=self.node_id,
            receiver=message.sender,
            payload={
                "error": f"Unknown or invalid message type: {message.type}",
            },
        )