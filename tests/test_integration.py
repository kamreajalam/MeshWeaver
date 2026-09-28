import asyncio

import pytest

from meshweaver.config import NodeConfig
from meshweaver.node import Node


def add(a, b):
    return a + b


async def start_node(node_id, bootstrap=None, **overrides):
    config = NodeConfig(
        node_id=node_id, host="127.0.0.1", port=0,
        gossip_interval=overrides.pop("gossip_interval", 100.0),
        heartbeat_interval=overrides.pop("heartbeat_interval", 100.0),
        heartbeat_timeout=overrides.pop("heartbeat_timeout", 0.2),
        failure_threshold=overrides.pop("failure_threshold", 2),
        max_task_retries=overrides.pop("max_task_retries", 2),
        task_timeout=overrides.pop("task_timeout", 2.0),
        **overrides,
    )
    node = Node(config, bootstrap_peers=bootstrap or [])
    await node.start()
    return node


@pytest.mark.asyncio
async def test_three_node_mesh_discovery():
    a = await start_node("mesh_a")
    try:
        addr_a = ("127.0.0.1", a.transport.bound_port)
        b = await start_node("mesh_b", bootstrap=[addr_a])
        try:
            await asyncio.sleep(0.2)
            addr_b = ("127.0.0.1", b.transport.bound_port)
            c = await start_node("mesh_c", bootstrap=[addr_b])
            try:
                await asyncio.sleep(0.3)
                # c bootstrapped only via b, but should learn about a too
                # via b's PEER_RESPONSE-style discovery (HELLO exchange +
                # peer list sharing).
                assert "mesh_b" in c.peers
                assert "mesh_a" in b.peers
                assert "mesh_b" in a.peers
            finally:
                await c.stop()
        finally:
            await b.stop()
    finally:
        await a.stop()


@pytest.mark.asyncio
async def test_gossip_propagates_cpu_between_nodes():
    a = await start_node("gossip_a", gossip_interval=0.05)
    b = await start_node("gossip_b", gossip_interval=0.05)
    try:
        a.peers.add_peer("gossip_b", "127.0.0.1", b.transport.bound_port)
        b.peers.add_peer("gossip_a", "127.0.0.1", a.transport.bound_port)

        await asyncio.sleep(0.3)

        peer_of_b_seen_by_a = a.peers.get_peer("gossip_b")
        assert peer_of_b_seen_by_a is not None
        assert peer_of_b_seen_by_a.cpu is not None
    finally:
        await a.stop()
        await b.stop()


@pytest.mark.asyncio
async def test_heartbeat_detects_node_failure():
    a = await start_node(
        "hb_a", heartbeat_interval=0.05, heartbeat_timeout=0.05, failure_threshold=2,
    )
    b = await start_node("hb_b")
    try:
        a.peers.add_peer("hb_b", "127.0.0.1", b.transport.bound_port)

        # b responds normally at first
        await asyncio.sleep(0.15)
        assert a.peers.get_peer("hb_b").status == "ALIVE"

        # Now stop b entirely -- it stops answering heartbeats.
        await b.stop()
        await asyncio.sleep(0.4)

        assert a.peers.get_peer("hb_b").status == "DEAD"
    finally:
        await a.stop()


@pytest.mark.asyncio
async def test_task_rerouted_to_available_node_after_failure():
    """Submit a task; the first candidate node is stopped, so submit_task
    must detect the failed send (timeout) and reroute to the still-alive
    node, ultimately returning a correct result.
    """
    a = await start_node("reroute_a", task_timeout=0.3, max_task_retries=2)
    b = await start_node("reroute_b")  # will be stopped mid-flight
    c = await start_node("reroute_c")  # stays alive
    try:
        # Give b the lowest cpu so the scheduler prefers it first.
        a.peers.add_peer("reroute_b", "127.0.0.1", b.transport.bound_port, cpu=5.0)
        a.peers.add_peer("reroute_c", "127.0.0.1", c.transport.bound_port, cpu=50.0)

        await b.stop()  # node_b is now unreachable

        result = await a.submit_task(add, (10, 20), timeout=0.3)
        assert result == {"status": "success", "result": 30}
    finally:
        await a.stop()
        await c.stop()


@pytest.mark.asyncio
async def test_multi_node_communication_broadcast_style():
    """One node pings several peers concurrently over real sockets."""
    a = await start_node("multi_a")
    b = await start_node("multi_b")
    c = await start_node("multi_c")
    try:
        addr_b = ("127.0.0.1", b.transport.bound_port)
        addr_c = ("127.0.0.1", c.transport.bound_port)

        results = await asyncio.gather(
            a.ping(addr_b, timeout=2.0),
            a.ping(addr_c, timeout=2.0),
        )
        assert all(results)
        assert "multi_b" in a.peers
        assert "multi_c" in a.peers
    finally:
        await a.stop()
        await b.stop()
        await c.stop()
