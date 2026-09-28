import pytest

from meshweaver.peer import PeerManager
from meshweaver.scheduler import NoAvailableNodeError, Scheduler


def make_manager():
    pm = PeerManager()
    pm.add_peer("node_a", "127.0.0.1", 5001, cpu=80.0, memory=50.0)
    pm.add_peer("node_b", "127.0.0.1", 5002, cpu=25.0, memory=40.0)
    pm.add_peer("node_c", "127.0.0.1", 5003, cpu=45.0, memory=30.0)
    return pm


def test_select_node_picks_least_loaded():
    pm = make_manager()
    scheduler = Scheduler(pm)
    chosen = scheduler.select_node()
    assert chosen.node_id == "node_b"  # lowest CPU


def test_select_node_deterministic_tie_break():
    pm = PeerManager()
    pm.add_peer("node_z", "127.0.0.1", 5001, cpu=10.0, memory=10.0)
    pm.add_peer("node_a", "127.0.0.1", 5002, cpu=10.0, memory=10.0)
    scheduler = Scheduler(pm)
    chosen = scheduler.select_node()
    assert chosen.node_id == "node_a"  # ties broken by node_id order


def test_select_node_raises_when_no_peers():
    pm = PeerManager()
    scheduler = Scheduler(pm)
    with pytest.raises(NoAvailableNodeError):
        scheduler.select_node()


def test_select_node_excludes_dead_peers():
    pm = make_manager()
    pm.update_status("node_b", "DEAD")
    scheduler = Scheduler(pm)
    chosen = scheduler.select_node()
    assert chosen.node_id == "node_c"  # next lowest CPU among alive peers


def test_reroute_picks_different_target():
    pm = make_manager()
    scheduler = Scheduler(pm, max_retries=2)
    first = scheduler.assign("task-1")
    second = scheduler.reroute("task-1")
    assert second is not None
    assert second.node_id != first.node_id


def test_reroute_gives_up_after_max_retries():
    pm = make_manager()
    scheduler = Scheduler(pm, max_retries=1)
    scheduler.assign("task-1")
    scheduler.reroute("task-1")  # attempt 2, within limit
    result = scheduler.reroute("task-1")  # would be attempt 3, exceeds max
    assert result is None
