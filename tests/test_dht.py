from meshweaver.dht import DHT, distance, node_id_for_address


def test_node_id_deterministic():
    id1 = node_id_for_address("127.0.0.1", 5000)
    id2 = node_id_for_address("127.0.0.1", 5000)
    assert id1 == id2


def test_node_id_differs_by_port():
    id1 = node_id_for_address("127.0.0.1", 5000)
    id2 = node_id_for_address("127.0.0.1", 5001)
    assert id1 != id2


def test_xor_distance_properties():
    a = 0b1010
    b = 0b0110
    assert distance(a, a) == 0
    assert distance(a, b) == distance(b, a)  # symmetric
    assert distance(a, b) == (a ^ b)


def test_dht_observe_and_find_nearest():
    dht = DHT("node_a", "127.0.0.1", 5000)
    dht.observe_peer("node_b", "127.0.0.1", 5001)
    dht.observe_peer("node_c", "127.0.0.1", 5002)
    dht.observe_peer("node_d", "127.0.0.1", 5003)

    nearest = dht.find_nearest("127.0.0.1", 5001, count=2)
    assert len(nearest) <= 2
    # the peer itself should be its own nearest match
    assert any(entry[2] == "node_b" for entry in nearest)


def test_dht_forget_peer():
    dht = DHT("node_a", "127.0.0.1", 5000)
    dht.observe_peer("node_b", "127.0.0.1", 5001)
    assert len(dht.known_peers()) == 1
    dht.forget_peer("127.0.0.1", 5001)
    assert len(dht.known_peers()) == 0


def test_dht_does_not_add_self():
    dht = DHT("node_a", "127.0.0.1", 5000)
    dht.observe_peer("node_a", "127.0.0.1", 5000)
    assert len(dht.known_peers()) == 0
