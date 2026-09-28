import pytest

from meshweaver.protocol import Message, PING
from meshweaver.security import (
    SecurityError,
    authenticate,
    attach_signature,
    decode_and_validate,
    is_valid_node_id,
    validate_message,
    verify_signature,
)


def test_valid_node_ids():
    assert is_valid_node_id("node_a")
    assert is_valid_node_id("node-1.local")
    assert not is_valid_node_id("")
    assert not is_valid_node_id("node with spaces")
    assert not is_valid_node_id("../../etc/passwd")


def test_validate_message_accepts_good_message():
    msg = Message(type=PING, sender="node_a", receiver="node_b")
    validate_message(msg)  # should not raise


def test_validate_message_rejects_bad_sender():
    msg = Message(type=PING, sender="node_a", receiver="node_b")
    msg.sender = "bad sender!"
    with pytest.raises(SecurityError):
        validate_message(msg)


def test_decode_and_validate_rejects_garbage():
    with pytest.raises(SecurityError):
        decode_and_validate(b"totally not a message")


def test_signature_roundtrip():
    msg = Message(type=PING, sender="node_a", receiver="node_b")
    attach_signature(msg, "supersecret")
    assert verify_signature(msg, "supersecret")
    assert not verify_signature(msg, "wrongsecret")


def test_authenticate_optional_when_not_required():
    msg = Message(type=PING, sender="node_a", receiver="node_b")
    authenticate(msg, None, required=False)  # should not raise


def test_authenticate_required_rejects_unsigned():
    msg = Message(type=PING, sender="node_a", receiver="node_b")
    with pytest.raises(SecurityError):
        authenticate(msg, "secret", required=True)


def test_authenticate_required_accepts_signed():
    msg = Message(type=PING, sender="node_a", receiver="node_b")
    attach_signature(msg, "secret")
    authenticate(msg, "secret", required=True)  # should not raise
