"""
meshweaver.security
=====================

Message-level validation and (optional) shared-secret authentication.

This module is intentionally honest about its limits: it protects against
malformed/malicious *envelopes* and can require a shared-secret HMAC on the
wire, but it does NOT make it safe to execute arbitrary cloudpickle payloads
from untrusted senders. See the module docstring in `serializer.py` and the
"Security limitations" section of the README for the full picture.

What this module actually provides:
    - Structural validation of decoded messages (already partly enforced by
      Message.__post_init__, this adds cross-field and node-id checks).
    - Rejection of unsupported message types.
    - Optional HMAC-SHA256 signing/verification of the message envelope,
      keyed by a shared secret (MESHWEAVER_SHARED_SECRET / NodeConfig).
    - Basic node-id sanity checks to reject obviously-forged/garbage ids.

What it does NOT provide:
    - Sandboxing of task execution. A TASK message's payload is a
      cloudpickle blob; deserializing and executing it runs arbitrary code
      with the executor process's privileges. Only accept tasks from nodes
      you've authenticated and trust.
    - Transport encryption. TLS is discussed but not implemented here (see
      README security section) — traffic is plaintext UDP/TCP.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from typing import Optional

from meshweaver.protocol import Message, ProtocolError, VALID_MESSAGE_TYPES

_NODE_ID_RE = re.compile(r"^[A-Za-z0-9_\-\.]{1,128}$")


class SecurityError(Exception):
    """Raised when a message fails validation or authentication."""


def is_valid_node_id(node_id: str) -> bool:
    """A node_id must be a short, printable token — no path separators,
    whitespace, or control characters. This blocks obviously-malicious or
    accidental garbage ids without pretending to authenticate identity.
    """
    return bool(node_id) and bool(_NODE_ID_RE.match(node_id))


def validate_message(message: Message) -> None:
    """Raise SecurityError if the message is structurally unsafe.

    Message.__post_init__ already guarantees type/sender/receiver/payload
    basics; this layer adds the checks that matter for a Router receiving
    data from the network rather than constructing it locally.
    """
    if message.type not in VALID_MESSAGE_TYPES:
        raise SecurityError(f"Rejected unsupported message type: {message.type!r}")

    if message.sender != "*" and not is_valid_node_id(message.sender):
        raise SecurityError(f"Rejected message with invalid sender id: {message.sender!r}")

    if message.receiver != "*" and not is_valid_node_id(message.receiver):
        raise SecurityError(f"Rejected message with invalid receiver id: {message.receiver!r}")

    if not isinstance(message.payload, dict):
        raise SecurityError("Rejected message with non-dict payload")

    # Metadata inside payload should never itself be trusted to contain
    # executable content or override protocol-level fields.
    if "__class__" in message.payload or "__reduce__" in message.payload:
        raise SecurityError("Rejected payload with suspicious dunder keys")


def decode_and_validate(raw: bytes) -> Message:
    """Decode raw bytes into a Message and validate it, raising
    SecurityError (not ProtocolError) on any failure so callers have a
    single exception type to catch at the network boundary.
    """
    try:
        message = Message.from_bytes(raw)
    except ProtocolError as exc:
        raise SecurityError(f"Rejected malformed message: {exc}") from exc

    validate_message(message)
    return message


# ---------------------------------------------------------------------------
# Optional shared-secret authentication (HMAC over the message id + type +
# sender + receiver + timestamp). This is deliberately simple: it proves the
# sender knows the shared secret, which is enough for a closed/trusted mesh,
# but is not a substitute for real per-node keys / TLS in a hostile network.
# ---------------------------------------------------------------------------

def sign_message(message: Message, secret: str) -> str:
    base = f"{message.message_id}|{message.type}|{message.sender}|{message.receiver}|{message.timestamp}"
    return hmac.new(secret.encode("utf-8"), base.encode("utf-8"), hashlib.sha256).hexdigest()


def attach_signature(message: Message, secret: str) -> Message:
    message.payload["_sig"] = sign_message(message, secret)
    return message


def verify_signature(message: Message, secret: str) -> bool:
    provided = message.payload.get("_sig")
    if not provided:
        return False
    expected = sign_message(message, secret)
    return hmac.compare_digest(provided, expected)


def authenticate(message: Message, secret: Optional[str], required: bool) -> None:
    """Enforce authentication policy for an incoming message.

    If `required` is False, this is a no-op (open/dev mesh).
    If `required` is True, the message must carry a valid HMAC signature
    computed with `secret`.
    """
    if not required:
        return
    if not secret:
        raise SecurityError("Authentication required but no shared secret configured")
    if not verify_signature(message, secret):
        raise SecurityError(f"Message {message.message_id} failed authentication")
