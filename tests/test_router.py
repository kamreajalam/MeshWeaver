import pytest

from meshweaver.protocol import Message, PING, PONG, HELLO
from meshweaver.router import Router


def test_route_ping_returns_pong():
    router = Router("node_b")
    ping = Message(type=PING, sender="node_a", receiver="node_b")
    response = router.route(ping)
    assert response is not None
    assert response.type == PONG
    assert response.correlation_id == ping.message_id


def test_route_unregistered_type_returns_none():
    router = Router("node_b")
    msg = Message(type=HELLO, sender="node_a", receiver="node_b")
    assert router.route(msg) is None


def test_route_registered_sync_handler():
    router = Router("node_b")
    calls = []

    def handler(message, addr):
        calls.append(message.type)
        return message.make_reply(PONG)

    router.register_handler(HELLO, handler)
    msg = Message(type=HELLO, sender="node_a", receiver="node_b")
    response = router.route(msg)
    assert calls == [HELLO]
    assert response.type == PONG


@pytest.mark.asyncio
async def test_dispatch_calls_async_handler():
    router = Router("node_b")
    received = {}

    async def handler(message, addr):
        received["message"] = message
        received["addr"] = addr

    router.register_handler(HELLO, handler)
    msg = Message(type=HELLO, sender="node_a", receiver="node_b")
    await router.dispatch(msg, ("127.0.0.1", 5000))

    assert received["message"].type == HELLO
    assert received["addr"] == ("127.0.0.1", 5000)


@pytest.mark.asyncio
async def test_dispatch_handler_exception_does_not_propagate():
    router = Router("node_b")

    async def bad_handler(message, addr):
        raise RuntimeError("boom")

    router.register_handler(HELLO, bad_handler)
    msg = Message(type=HELLO, sender="node_a", receiver="node_b")
    await router.dispatch(msg, ("127.0.0.1", 5000))  # should not raise
