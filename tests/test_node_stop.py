import asyncio
import pytest

from meshweaver.config import NodeConfig
from meshweaver.node import Node, NodeState
from meshweaver.protocol import NODE_STOP, Message


@pytest.mark.asyncio
async def test_remote_node_stop_graceful():
    """Verify that a remote NODE_STOP command gracefully shuts down the target node."""
    config_target = NodeConfig(node_id="target_node", host="127.0.0.1", port=0)
    target = Node(config_target)
    await target.start()
    addr_target = ("127.0.0.1", target.transport.bound_port)

    config_client = NodeConfig(node_id="client_node", host="127.0.0.1", port=0)
    client = Node(config_client)
    await client.start()

    try:
        assert target.state == NodeState.RUNNING
        assert not target._stop_event.is_set()

        ok = await client.stop_remote_node(addr_target, timeout=3.0)
        assert ok is True

        # Wait for graceful delayed stop
        await asyncio.sleep(0.2)
        assert target.state == NodeState.STOPPED
        assert target._stop_event.is_set()
        assert target.transport is None
    finally:
        await client.stop()
        if target.state != NodeState.STOPPED:
            await target.stop()


@pytest.mark.asyncio
async def test_node_stop_and_cancel_require_auth():
    """When require_auth=True, unsigned NODE_STOP and TASK_CANCEL requests are rejected."""
    secret = "meshweaver-secret-key"
    config_secure = NodeConfig(
        node_id="secure_node", host="127.0.0.1", port=0,
        require_auth=True, shared_secret=secret,
    )
    target = Node(config_secure)
    await target.start()
    addr_target = ("127.0.0.1", target.transport.bound_port)

    # Attacker node without authentication
    config_attacker = NodeConfig(node_id="attacker", host="127.0.0.1", port=0, require_auth=False)
    attacker = Node(config_attacker)
    await attacker.start()

    # Legitimate node with correct shared secret
    config_legit = NodeConfig(node_id="legit", host="127.0.0.1", port=0, require_auth=True, shared_secret=secret)
    legit = Node(config_legit)
    await legit.start()

    try:
        # Attacker tries to stop target without signature -> times out (rejected by Router)
        stop_msg = Message(type=NODE_STOP, sender="attacker", receiver="*")
        resp = await attacker._request_with_timeout(stop_msg, addr_target, timeout=0.3)
        assert resp is None
        assert target.state == NodeState.RUNNING

        # Legitimate client stops target with valid signature -> succeeds
        ok = await legit.stop_remote_node(addr_target, timeout=2.0)
        assert ok is True
        await asyncio.sleep(0.15)
        assert target.state == NodeState.STOPPED
    finally:
        await attacker.stop()
        await legit.stop()
        if target.state != NodeState.STOPPED:
            await target.stop()
