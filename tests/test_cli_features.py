import argparse
import pytest

from meshweaver.cli import cmd_submit, cmd_task_status
from meshweaver.config import NodeConfig
from meshweaver.node import Node


@pytest.mark.asyncio
async def test_cli_submit_explicit_target(capsys):
    """Test CLI submit targeting a specific node."""
    config_b = NodeConfig(node_id="cli_target_b", host="127.0.0.1", port=0)
    node_b = Node(config_b)
    await node_b.start()
    addr_b = ("127.0.0.1", node_b.transport.bound_port)

    try:
        args = argparse.Namespace(
            target="cli_target_b",
            target_host=addr_b[0],
            target_port=addr_b[1],
            host="127.0.0.1",
            port=None,
            function="examples.simple_task:add",
            args=["10", "20"],
            args_json=None,
            kwargs_json=None,
            timeout=5.0,
        )
        rc = await cmd_submit(args)
        assert rc == 0

        captured = capsys.readouterr().out
        assert "Task ID:" in captured
        assert "Target: cli_target_b" in captured
        assert "Status: COMPLETED" in captured
        assert "Result: 30" in captured
    finally:
        await node_b.stop()


@pytest.mark.asyncio
async def test_cli_submit_automatic_scheduling(capsys):
    """Test CLI submit without --target: coordinator discovers peers and scheduler selects worker."""
    # Worker 1 (high CPU)
    config_w1 = NodeConfig(node_id="worker_busy", host="127.0.0.1", port=0)
    worker_1 = Node(config_w1)
    await worker_1.start()

    # Worker 2 (low CPU)
    config_w2 = NodeConfig(node_id="worker_idle", host="127.0.0.1", port=0)
    worker_2 = Node(config_w2)
    await worker_2.start()

    # Coordinator
    config_coord = NodeConfig(node_id="coord", host="127.0.0.1", port=0)
    coord = Node(config_coord)
    await coord.start()
    coord_addr = ("127.0.0.1", coord.transport.bound_port)

    try:
        # Add workers to coordinator with metrics
        coord.peers.add_peer("worker_busy", "127.0.0.1", worker_1.transport.bound_port, cpu=85.0)
        coord.peers.add_peer("worker_idle", "127.0.0.1", worker_2.transport.bound_port, cpu=12.0)

        args = argparse.Namespace(
            target=None,
            target_host=None,
            target_port=None,
            host=coord_addr[0],
            port=coord_addr[1],
            function="examples.simple_task:add",
            args=None,
            args_json="[100, 200]",
            kwargs_json=None,
            timeout=5.0,
        )
        rc = await cmd_submit(args)
        assert rc == 0

        captured = capsys.readouterr().out
        assert "Task ID:" in captured
        # Scheduler must pick worker_idle because it has 12% CPU vs 85%
        assert "Target: worker_idle" in captured
        assert "Status: COMPLETED" in captured
        assert "Result: 300" in captured
    finally:
        await coord.stop()
        await worker_1.stop()
        await worker_2.stop()


@pytest.mark.asyncio
async def test_cli_task_status_and_cancel(capsys):
    """Test CLI task status and task cancel commands."""
    config = NodeConfig(node_id="task_cli_node", host="127.0.0.1", port=0)
    node = Node(config)
    await node.start()
    addr = ("127.0.0.1", node.transport.bound_port)

    try:
        # Register node as peer so target="task_cli_node" is reachable
        node.peers.add_peer("task_cli_node", addr[0], addr[1])
        # Submit a task via node API
        res = await node.submit_task(
            _slow_func, (2,), target="task_cli_node"
        )
        tid = res.task_id

        # Query status via CLI
        args_status = argparse.Namespace(
            host=addr[0],
            port=addr[1],
            task_id=tid,
            timeout=2.0,
        )
        rc_status = await cmd_task_status(args_status)
        assert rc_status == 0
        captured_status = capsys.readouterr().out
        assert f"task_id : {tid}" in captured_status
        assert "COMPLETED" in captured_status
    finally:
        await node.stop()


def _slow_func(val):
    return val * 10
