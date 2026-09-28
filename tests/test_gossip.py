import asyncio

import pytest

from meshweaver.gossip import GossipManager
from meshweaver.peer import PeerManager


@pytest.mark.asyncio
async def test_gossip_sends_to_known_peers():
    pm = PeerManager()
    pm.add_peer("node_b", "127.0.0.1", 5001)
    pm.add_peer("node_c", "127.0.0.1", 5002)

    sent = []

    async def fake_send(status, addr):
        sent.append((status, addr))

    gm = GossipManager("node_a", pm, fake_send, interval=0.05, fanout=5)
    gm.start()
    await asyncio.sleep(0.12)
    await gm.stop()

    assert len(sent) >= 2  # at least one round to each of 2 peers


@pytest.mark.asyncio
async def test_gossip_stop_cancels_cleanly():
    pm = PeerManager()

    async def fake_send(status, addr):
        pass

    gm = GossipManager("node_a", pm, fake_send, interval=0.05)
    gm.start()
    await gm.stop()
    # calling stop twice should be safe
    await gm.stop()


def test_handle_gossip_updates_known_peer():
    pm = PeerManager()
    pm.add_peer("node_b", "127.0.0.1", 5001)

    async def fake_send(status, addr):
        pass

    gm = GossipManager("node_a", pm, fake_send)
    gm.handle_gossip({"node_id": "node_b", "cpu": 33.3, "memory": 55.5})

    peer = pm.get_peer("node_b")
    assert peer.cpu == 33.3
    assert peer.memory == 55.5


def test_handle_gossip_ignores_unknown_peer():
    pm = PeerManager()

    async def fake_send(status, addr):
        pass

    gm = GossipManager("node_a", pm, fake_send)
    gm.handle_gossip({"node_id": "node_x", "cpu": 10.0, "memory": 10.0})
    assert pm.get_peer("node_x") is None


def test_handle_gossip_ignores_self():
    pm = PeerManager()

    async def fake_send(status, addr):
        pass

    gm = GossipManager("node_a", pm, fake_send)
    gm.handle_gossip({"node_id": "node_a", "cpu": 10.0, "memory": 10.0})  # should be a no-op
