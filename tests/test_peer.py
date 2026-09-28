from meshweaver.peer import PeerManager


def test_add_and_get_peer():
    pm = PeerManager()
    pm.add_peer("node_b", "127.0.0.1", 5001)
    peer = pm.get_peer("node_b")
    assert peer is not None
    assert peer.host == "127.0.0.1"
    assert peer.port == 5001
    assert peer.status == "ALIVE"


def test_add_peer_deduplicates():
    pm = PeerManager()
    pm.add_peer("node_b", "127.0.0.1", 5001)
    pm.add_peer("node_b", "127.0.0.1", 5001)
    assert len(pm) == 1


def test_remove_peer():
    pm = PeerManager()
    pm.add_peer("node_b", "127.0.0.1", 5001)
    assert pm.remove_peer("node_b") is True
    assert pm.get_peer("node_b") is None
    assert pm.remove_peer("node_b") is False


def test_get_peers_alive_only():
    pm = PeerManager()
    pm.add_peer("node_b", "127.0.0.1", 5001)
    pm.add_peer("node_c", "127.0.0.1", 5002)
    pm.update_status("node_c", "DEAD")
    alive = pm.get_peers(alive_only=True)
    assert len(alive) == 1
    assert alive[0].node_id == "node_b"


def test_update_metrics():
    pm = PeerManager()
    pm.add_peer("node_b", "127.0.0.1", 5001)
    pm.update_metrics("node_b", cpu=45.0, memory=60.0)
    peer = pm.get_peer("node_b")
    assert peer.cpu == 45.0
    assert peer.memory == 60.0


def test_touch_resets_missed_heartbeats():
    pm = PeerManager()
    peer = pm.add_peer("node_b", "127.0.0.1", 5001)
    peer.missed_heartbeats = 2
    peer.status = "SUSPECT"
    peer.touch()
    assert peer.missed_heartbeats == 0
    assert peer.status == "ALIVE"
