import asyncio
import time
import pytest

from meshweaver.config import NodeConfig
from meshweaver.executor import TaskState
from meshweaver.node import Node
from meshweaver.serializer import serialize


def fast_add(a, b):
    return a + b


def slow_task(seconds):
    time.sleep(seconds)
    return "done"


def fail_task():
    raise RuntimeError("intentional task failure")


@pytest.mark.asyncio
async def test_task_status_states_and_query():
    """Verify genuine task states across completion, failure, and status query."""
    config_a = NodeConfig(node_id="lc_node_a", host="127.0.0.1", port=0,
                          gossip_interval=100.0, heartbeat_interval=100.0)
    config_b = NodeConfig(node_id="lc_node_b", host="127.0.0.1", port=0,
                          gossip_interval=100.0, heartbeat_interval=100.0)
    node_a = Node(config_a)
    node_b = Node(config_b)
    await node_a.start()
    await node_b.start()
    try:
        node_a.peers.add_peer("lc_node_b", "127.0.0.1", node_b.transport.bound_port)

        # 1. Success task
        res = await node_a.submit_task(fast_add, (5, 7), target="lc_node_b")
        assert res["status"] == "success"
        assert res["result"] == 12
        tid = res.task_id

        # Query status from coordinator
        status_a = await node_a.query_remote_status(
            ("127.0.0.1", node_a.transport.bound_port), task_id=tid
        )
        assert status_a is not None
        assert status_a["state"] == "COMPLETED"
        assert status_a["result"] == 12

        # Query status from worker
        status_b = await node_a.query_remote_status(
            ("127.0.0.1", node_b.transport.bound_port), task_id=tid
        )
        assert status_b is not None
        assert status_b["state"] == "COMPLETED"
        assert status_b["result"] == 12

        # 2. Failed task
        res_fail = await node_a.submit_task(fail_task, (), target="lc_node_b")
        assert res_fail["status"] == "error"
        status_fail = await node_a.query_remote_status(
            ("127.0.0.1", node_a.transport.bound_port), task_id=res_fail.task_id
        )
        assert status_fail is not None
        assert status_fail["state"] == "FAILED"
    finally:
        await node_a.stop()
        await node_b.stop()


@pytest.mark.asyncio
async def test_task_cancellation_running_and_completed():
    """Verify task cancellation on a running task, and that completed tasks cannot be cancelled."""
    config = NodeConfig(node_id="cancel_node", host="127.0.0.1", port=0,
                        gossip_interval=100.0, heartbeat_interval=100.0)
    node = Node(config)
    await node.start()
    try:
        # Submit a slow task to executor
        payload = serialize(slow_task, (3,), {})
        tid = await node.executor.submit_task(payload)
        await asyncio.sleep(0.05)
        assert node.executor.get_task_status(tid) == TaskState.RUNNING

        # Cancel running task via remote cancel message
        addr = ("127.0.0.1", node.transport.bound_port)
        cancel_resp = await node.cancel_remote_task(addr, task_id=tid, timeout=2.0)
        assert cancel_resp["cancelled"] is True
        assert node.executor.get_task_status(tid) == TaskState.CANCELLED

        # Query status
        status = await node.query_remote_status(addr, task_id=tid)
        assert status["state"] == "CANCELLED"

        # Submit a fast task that completes
        payload_fast = serialize(fast_add, (1, 2), {})
        fast_tid = await node.executor.submit_task(payload_fast)
        await asyncio.sleep(0.1)
        assert node.executor.get_task_status(fast_tid) == TaskState.COMPLETED

        # Attempt to cancel completed task -> must be rejected
        cancel_fast_resp = await node.cancel_remote_task(addr, task_id=fast_tid, timeout=2.0)
        assert cancel_fast_resp["cancelled"] is False
        assert "already COMPLETED" in cancel_fast_resp["reason"]
        assert node.executor.get_task_status(fast_tid) == TaskState.COMPLETED
    finally:
        await node.stop()
