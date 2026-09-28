import asyncio

import pytest

from meshweaver.executor import Executor, TaskState
from meshweaver.serializer import serialize


def add(a, b):
    return a + b


def boom():
    raise ValueError("intentional failure")


def slow(seconds):
    import time
    time.sleep(seconds)
    return "done"


@pytest.mark.asyncio
async def test_execute_task_success():
    executor = Executor(max_workers=2, default_timeout=5)
    payload = serialize(add, (2, 3), {})
    result = await executor.execute_task(payload)
    assert result == {"status": "success", "result": 5}
    executor.shutdown()


@pytest.mark.asyncio
async def test_execute_task_exception_does_not_crash():
    executor = Executor(max_workers=2, default_timeout=5)
    payload = serialize(boom, (), {})
    result = await executor.execute_task(payload)
    assert result["status"] == "error"
    assert "intentional failure" in result["message"]
    executor.shutdown()


@pytest.mark.asyncio
async def test_execute_task_timeout():
    executor = Executor(max_workers=2, default_timeout=5)
    payload = serialize(slow, (2,), {})
    result = await executor.execute_task(payload, timeout=0.2)
    assert result["status"] == "error"
    assert result["state"] == TaskState.TIMEOUT.value
    executor.shutdown()


@pytest.mark.asyncio
async def test_malformed_payload_does_not_crash():
    executor = Executor(max_workers=2, default_timeout=5)
    result = await executor.execute_task(b"garbage-not-a-task")
    assert result["status"] == "error"
    executor.shutdown()


@pytest.mark.asyncio
async def test_get_task_status_and_result():
    executor = Executor(max_workers=2, default_timeout=5)
    payload = serialize(add, (10, 20), {})
    task_id = await executor.submit_task(payload)
    # wait for background task
    await asyncio.sleep(0.1)
    assert executor.get_task_status(task_id) == TaskState.COMPLETED
    assert executor.get_result(task_id) == {"status": "success", "result": 30}
    executor.shutdown()


@pytest.mark.asyncio
async def test_node_survives_multiple_failed_tasks():
    """A crashing task must never take down the event loop / executor."""
    executor = Executor(max_workers=2, default_timeout=5)
    for _ in range(5):
        payload = serialize(boom, (), {})
        result = await executor.execute_task(payload)
        assert result["status"] == "error"
    # executor should still work fine afterward
    payload = serialize(add, (1, 1), {})
    result = await executor.execute_task(payload)
    assert result == {"status": "success", "result": 2}
    executor.shutdown()
