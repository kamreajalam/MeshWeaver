from typing import Callable

from .protocol import (
    Message,
    PING,
    PONG,
    TASK,
    RESULT,
    ERROR,
)


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
        """Route an incoming message to the correct handler."""

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
            payload={
                "message": "PONG",
            },
        )

    def handle_task(self, message: Message) -> Message:
        """Handle TASK.

        The real executor will be connected later.
        """

        return Message(
            type=ERROR,
            sender=self.node_id,
            receiver=message.sender,
            payload={
                "error": "Task executor not connected yet",
            },
        )

    def handle_result(self, message: Message) -> Message:
        """Handle task result."""

        return message

    def handle_error(self, message: Message) -> Message:
        """Handle error message."""

        return message

    def handle_unknown(self, message: Message) -> Message:
        """Handle unsupported message types."""

        return Message(
            type=ERROR,
            sender=self.node_id,
            receiver=message.sender,
            payload={
                "error": f"Unknown message type: {message.type}",
            },
        )