from meshweaver.protocol import Message, PING, PONG
from meshweaver.router import Router


def test_ping_returns_pong():
    router = Router("node_b")

    ping = Message(
        type=PING,
        sender="node_a",
        receiver="node_b",
    )

    response = router.route(ping)

    assert response is not None
    assert response.type == PONG
    assert response.sender == "node_b"
    assert response.receiver == "node_a"


def test_unknown_message_returns_error():
    router = Router("node_b")

    message = Message(
        type="UNKNOWN",
        sender="node_a",
        receiver="node_b",
    )

    response = router.route(message)

    assert response is not None
    assert response.type == "ERROR"
    assert response.sender == "node_b"
    assert response.receiver == "node_a"