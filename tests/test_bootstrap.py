import asyncio
import pytest

from meshweaver.config import NodeConfig
from meshweaver.node import Node


@pytest.mark.asyncio
async def test_reliable_bootstrap_near_simultaneous():
    """Two nodes started nearly simultaneously over real UDP sockets reliably discover each other."""
    config_a = NodeConfig(node_id="node_sim_a", host="127.0.0.1", port=0,
                          gossip_interval=100.0, heartbeat_interval=100.0)
    node_a = Node(config_a)
    await node_a.start()
    addr_a = ("127.0.0.1", node_a.transport.bound_port)

    config_b = NodeConfig(node_id="node_sim_b", host="127.0.0.1", port=0,
                          gossip_interval=100.0, heartbeat_interval=100.0,
                          bootstrap_retries=5, bootstrap_timeout=0.5, bootstrap_retry_interval=0.1)
    node_b = Node(config_b, bootstrap_peers=[addr_a])
    await node_b.start()

    try:
        # Give a short moment for bootstrap acknowledgement exchange
        await asyncio.sleep(0.3)
        assert "node_sim_a" in node_b.peers
        assert "node_sim_b" in node_a.peers
    finally:
        await node_b.stop()
        await node_a.stop()


@pytest.mark.asyncio
async def test_transitive_discovery_bidirectional():
    """Node C bootstraps only via Node B; Node B knows Node A.
    Node C discovers Node A transitively, and Node A discovers Node C.
    """
    config_a = NodeConfig(node_id="trans_a", host="127.0.0.1", port=0,
                          gossip_interval=100.0, heartbeat_interval=100.0)
    node_a = Node(config_a)
    await node_a.start()
    addr_a = ("127.0.0.1", node_a.transport.bound_port)

    config_b = NodeConfig(node_id="trans_b", host="127.0.0.1", port=0,
                          gossip_interval=100.0, heartbeat_interval=100.0)
    node_b = Node(config_b, bootstrap_peers=[addr_a])
    await node_b.start()
    addr_b = ("127.0.0.1", node_b.transport.bound_port)

    # Let A and B discover each other
    await asyncio.sleep(0.2)
    assert "trans_a" in node_b.peers

    config_c = NodeConfig(node_id="trans_c", host="127.0.0.1", port=0,
                          gossip_interval=100.0, heartbeat_interval=100.0)
    node_c = Node(config_c, bootstrap_peers=[addr_b])
    await node_c.start()

    try:
        # Wait for transitive propagation
        await asyncio.sleep(0.5)
        # C must know B and A
        assert "trans_b" in node_c.peers
        assert "trans_a" in node_c.peers
        # A must also discover C transitively
        assert "trans_c" in node_a.peers
    finally:
        await node_c.stop()
        await node_b.stop()
        await node_a.stop()
