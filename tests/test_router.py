from meshweaver.protocol import ERROR, PING, PONG, RESULT, TASK, Message
from meshweaver.router import Router


def add(a, b):
    return a + b


def test_route_ping():
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
    assert response.type == ERROR
    assert response.sender == "node_b"
    assert response.receiver == "node_a"


def test_route_task_returns_executor_result(monkeypatch):
    router = Router("node_b")

    def fake_send_task(func, args):
        assert func is add
        assert args == (2, 3)
        return {"status": "success", "result": 5}

    monkeypatch.setattr("meshweaver.router.send_task", fake_send_task)

    task = Message(
        type=TASK,
        sender="node_a",
        receiver="node_b",
        payload={"function": add, "args": (2, 3)},
    )

    response = router.route(task)

    assert response is not None
    assert response.type == RESULT
    assert response.sender == "node_b"
    assert response.receiver == "node_a"
    assert response.payload == {"result": 5}


def test_route_task_rejects_invalid_payload():
    router = Router("node_b")

    task = Message(
        type=TASK,
        sender="node_a",
        receiver="node_b",
        payload={"function": "not-a-function", "args": (2, 3)},
    )

    response = router.route(task)

    assert response is not None
    assert response.type == ERROR
    assert "callable" in response.payload["error"]


def test_route_task_returns_error_when_executor_is_unavailable(monkeypatch):
    router = Router("node_b")

    monkeypatch.setattr(
        "meshweaver.router.send_task",
        lambda func, args: None,
    )

    task = Message(
        type=TASK,
        sender="node_a",
        receiver="node_b",
        payload={"function": add, "args": (2, 3)},
    )

    response = router.route(task)

    assert response is not None
    assert response.type == ERROR
    assert response.payload == {"error": "Executor is unavailable"}