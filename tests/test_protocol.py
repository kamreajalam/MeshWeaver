from meshweaver.protocol import (
    Message,
    PING,
    PONG,
    TASK,
    RESULT,
    ERROR,
)


def test_ping_message():
    message = Message(
        type=PING,
        sender="node_a",
        receiver="node_b",
    )

    assert message.type == PING
    assert message.sender == "node_a"
    assert message.receiver == "node_b"


def test_message_to_dict():
    message = Message(
        type=PONG,
        sender="node_b",
        receiver="node_a",
    )

    data = message.to_dict()

    assert data["type"] == PONG
    assert data["sender"] == "node_b"
    assert data["receiver"] == "node_a"


def test_message_from_dict():
    data = {
        "type": TASK,
        "sender": "node_a",
        "receiver": "node_b",
        "message_id": "task-001",
        "payload": {"function": "example"},
    }

    message = Message.from_dict(data)

    assert message.type == TASK
    assert message.sender == "node_a"
    assert message.receiver == "node_b"
    assert message.message_id == "task-001"
    assert message.payload == {"function": "example"}