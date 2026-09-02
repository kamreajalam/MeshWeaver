from .protocol import (
    Message,
    PING,
    PONG,
    TASK,
    RESULT,
    ERROR,
)


VALID_MESSAGE_TYPES = {
    PING,
    PONG,
    TASK,
    RESULT,
    ERROR,
}


def validate_message(message: Message) -> bool:
    """Validate a MeshWeaver message."""

    if not isinstance(message, Message):
        return False

    if message.type not in VALID_MESSAGE_TYPES:
        return False

    if not message.sender:
        return False

    if message.receiver is not None and not message.receiver:
        return False

    return True


def validate_node_id(node_id: str) -> bool:
    """Validate a node ID."""

    return (
        isinstance(node_id, str)
        and len(node_id.strip()) > 0
    )