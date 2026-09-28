import asyncio

import pytest

from meshweaver.config import NodeConfig
from meshweaver.node import Node, NodeState


def add(a, b):
    return a + b


def fails():
    raise RuntimeError("nope")


async def make_node(node_id, gossip_interval=100.0, heartbeat_interval=100.0):
    config = NodeConfig(
        node_id=node_id, host="127.0.0.1", port=0,
        gossip_interval=gossip_interval, heartbeat_interval=heartbeat_interval,
    )
    node = Node(config)
    await node.start()
    return node


@pytest.mark.asyncio
async def test_node_lifecycle():
    node = await make_node("node_lifecycle")
    assert node.state == NodeState.RUNNING
    await node.stop()
    assert node.state == NodeState.STOPPED


@pytest.mark.asyncio
async def test_real_udp_ping_pong_between_nodes():
    node_a = await make_node("node_a_ping")
    node_b = await make_node("node_b_ping")
    try:
        addr_b = ("127.0.0.1", node_b.transport.bound_port)
        ok = await node_a.ping(addr_b, timeout=2.0)
        assert ok is True
        assert "node_b_ping" in node_a.peers
    finally:
        await node_a.stop()
        await node_b.stop()


@pytest.mark.asyncio
async def test_peer_discovery_via_hello_bootstrap():
    node_a = await make_node("node_a_disc")
    try:
        addr_a = ("127.0.0.1", node_a.transport.bound_port)
        config_b = NodeConfig(node_id="node_b_disc", host="127.0.0.1", port=0,
                               gossip_interval=100.0, heartbeat_interval=100.0)
        node_b = Node(config_b, bootstrap_peers=[addr_a])
        await node_b.start()
        try:
            await asyncio.sleep(0.2)
            assert "node_a_disc" in node_b.peers
            assert "node_b_disc" in node_a.peers
        finally:
            await node_b.stop()
    finally:
        await node_a.stop()


@pytest.mark.asyncio
async def test_end_to_end_remote_task_execution():
    node_a = await make_node("node_a_task")
    node_b = await make_node("node_b_task")
    try:
        node_a.peers.add_peer("node_b_task", "127.0.0.1", node_b.transport.bound_port)
        result = await node_a.submit_task(add, (2, 3), target="node_b_task", timeout=5.0)
        assert result == {"status": "success", "result": 5}
    finally:
        await node_a.stop()
        await node_b.stop()


@pytest.mark.asyncio
async def test_remote_task_exception_returns_error_not_crash():
    node_a = await make_node("node_a_err")
    node_b = await make_node("node_b_err")
    try:
        node_a.peers.add_peer("node_b_err", "127.0.0.1", node_b.transport.bound_port)
        result = await node_a.submit_task(fails, (), target="node_b_err", timeout=5.0)
        assert result["status"] == "error"
        # node_b must still be alive and able to run another task
        result2 = await node_a.submit_task(add, (1, 1), target="node_b_err", timeout=5.0)
        assert result2 == {"status": "success", "result": 2}
    finally:
        await node_a.stop()
        await node_b.stop()


@pytest.mark.asyncio
async def test_scheduler_picks_least_loaded_when_no_target_given():
    node_a = await make_node("node_a_sched")
    node_b = await make_node("node_b_sched")
    node_c = await make_node("node_c_sched")
    try:
        node_a.peers.add_peer("node_b_sched", "127.0.0.1", node_b.transport.bound_port, cpu=80.0)
        node_a.peers.add_peer("node_c_sched", "127.0.0.1", node_c.transport.bound_port, cpu=10.0)

        result = await node_a.submit_task(add, (7, 8), timeout=5.0)
        assert result == {"status": "success", "result": 15}
    finally:
        await node_a.stop()
        await node_b.stop()
        await node_c.stop()


@pytest.mark.asyncio
async def test_remote_status_query():
    node_a = await make_node("node_a_status")
    node_b = await make_node("node_b_status")
    try:
        addr_b = ("127.0.0.1", node_b.transport.bound_port)
        status = await node_a.query_remote_status(addr_b, timeout=2.0)
        assert status is not None
        assert status["node_id"] == "node_b_status"
        assert status["state"] == "RUNNING"
    finally:
        await node_a.stop()
        await node_b.stop()
