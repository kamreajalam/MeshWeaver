from meshweaver.protocol import Message, PING
from meshweaver.security import (
    validate_message,
    validate_node_id,
)


def test_valid_message():
    message = Message(
        type=PING,
        sender="node_a",
        receiver="node_b",
    )

    assert validate_message(message) is True


def test_invalid_message_type():
    message = Message(
        type="INVALID",
        sender="node_a",
        receiver="node_b",
    )

    assert validate_message(message) is False


def test_valid_node_id():
    assert validate_node_id("node_a") is True


def test_invalid_node_id():
    assert validate_node_id("") is False