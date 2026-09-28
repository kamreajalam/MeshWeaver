import pytest

from meshweaver.protocol import Message, ProtocolError, PING, PONG, TASK


def test_message_roundtrip_dict():
    msg = Message(type=PING, sender="a", receiver="b")
    data = msg.to_dict()
    restored = Message.from_dict(data)
    assert restored.type == PING
    assert restored.sender == "a"
    assert restored.receiver == "b"
    assert restored.message_id == msg.message_id


def test_message_roundtrip_bytes():
    msg = Message(type=TASK, sender="a", receiver="b", payload={"x": 1})
    raw = msg.to_bytes()
    restored = Message.from_bytes(raw)
    assert restored.type == TASK
    assert restored.payload == {"x": 1}


def test_invalid_message_type_rejected():
    with pytest.raises(ProtocolError):
        Message(type="NOT_A_TYPE", sender="a", receiver="b")


def test_empty_sender_rejected():
    with pytest.raises(ProtocolError):
        Message(type=PING, sender="", receiver="b")


def test_from_dict_missing_fields():
    with pytest.raises(ProtocolError):
        Message.from_dict({"type": PING})


def test_from_bytes_malformed_json():
    with pytest.raises(ProtocolError):
        Message.from_bytes(b"not json at all {{{")


def test_from_bytes_empty():
    with pytest.raises(ProtocolError):
        Message.from_bytes(b"")


def test_make_reply_sets_correlation_id():
    ping = Message(type=PING, sender="a", receiver="b")
    pong = ping.make_reply(PONG)
    assert pong.correlation_id == ping.message_id
    assert pong.sender == "b"
    assert pong.receiver == "a"
    assert pong.type == PONG
