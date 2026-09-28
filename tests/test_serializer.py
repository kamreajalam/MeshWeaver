import pytest

from meshweaver.serializer import (
    SerializationError,
    Task,
    deserialize,
    serialize,
)


def add(a, b):
    return a + b


def greet(name, greeting="Hello"):
    return f"{greeting}, {name}!"


def test_serialize_simple_function():
    data = serialize(add, (2, 3), {})
    func, args, kwargs = deserialize(data)
    assert func(*args, **kwargs) == 5


def test_serialize_with_kwargs():
    data = serialize(greet, ("World",), {"greeting": "Hi"})
    func, args, kwargs = deserialize(data)
    assert func(*args, **kwargs) == "Hi, World!"


def test_serialize_does_not_execute():
    calls = []

    def tracked():
        calls.append(1)
        return 42

    serialize(tracked, (), {})
    assert calls == []  # serializing must never call the function


def test_serialize_closure():
    multiplier = 3

    def scale(x):
        return x * multiplier

    data = serialize(scale, (10,), {})
    func, args, kwargs = deserialize(data)
    assert func(*args, **kwargs) == 30


def test_serialize_structured_data():
    def process(d):
        return {k: v * 2 for k, v in d.items()}

    data = serialize(process, ({"a": 1, "b": 2},), {})
    func, args, kwargs = deserialize(data)
    assert func(*args, **kwargs) == {"a": 2, "b": 4}


def test_deterministic_complex_function():
    def fib(n):
        a, b = 0, 1
        for _ in range(n):
            a, b = b, a + b
        return a

    data = serialize(fib, (10,), {})
    func, args, kwargs = deserialize(data)
    assert func(*args, **kwargs) == 55


def test_deserialize_garbage_raises():
    with pytest.raises(SerializationError):
        deserialize(b"not a valid cloudpickle blob")


def test_task_create_and_message_payload_roundtrip():
    task = Task.create(add, (4, 5), sender="node_a", target="node_b")
    payload = task.to_message_payload()
    restored = Task.from_message_payload(payload)

    func, args, kwargs = restored.deserialize_call()
    assert func(*args, **kwargs) == 9
    assert restored.task_id == task.task_id
    assert restored.sender == "node_a"
    assert restored.target == "node_b"
