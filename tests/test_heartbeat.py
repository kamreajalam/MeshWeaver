import asyncio

import pytest

from meshweaver.heartbeat import HeartbeatManager
from meshweaver.peer import PeerManager


@pytest.mark.asyncio
async def test_heartbeat_acked_keeps_peer_alive():
    pm = PeerManager()
    pm.add_peer("node_b", "127.0.0.1", 5001)

    hb = HeartbeatManager("node_a", pm, send_heartbeat=lambda addr: asyncio.sleep(0),
                           interval=0.05, timeout=0.05, failure_threshold=2)

    stop_responding = asyncio.Event()

    async def responder():
        # Simulate node_b continuously ACKing every heartbeat round for as
        # long as the test is running.
        while not stop_responding.is_set():
            hb.handle_ack("node_b")
            await asyncio.sleep(0.02)

    hb.start()
    task = asyncio.create_task(responder())
    await asyncio.sleep(0.2)
    stop_responding.set()
    await hb.stop()
    await task

    peer = pm.get_peer("node_b")
    assert peer.status == "ALIVE"


@pytest.mark.asyncio
async def test_heartbeat_marks_peer_dead_after_threshold():
    pm = PeerManager()
    pm.add_peer("node_b", "127.0.0.1", 5001)

    failed = []
    hb = HeartbeatManager(
        "node_a", pm, send_heartbeat=lambda addr: asyncio.sleep(0),
        interval=0.02, timeout=0.02, failure_threshold=2,
        on_peer_failed=lambda nid: failed.append(nid),
    )
    # node_b never ACKs -> should be marked DEAD after 2 missed rounds
    hb.start()
    await asyncio.sleep(0.3)
    await hb.stop()

    peer = pm.get_peer("node_b")
    assert peer.status == "DEAD"
    assert "node_b" in failed
