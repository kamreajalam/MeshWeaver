"""
meshweaver.router
====================

The Router is the central message dispatcher: decoded, validated messages
come in, get authenticated if required, and are handed to a per-type
handler. Handlers are registered by Node (and by tests / app.py) rather
than hardcoded here, so router.py stays a thin dispatcher instead of
absorbing the whole application.

Backward compatibility: the original app.py prototype used a synchronous
`Router(node_id).route(ping_message) -> pong_message` API for a local,
in-process PING/PONG demo with no real networking. That entry point still
works unchanged (`route()` below) — it's a convenience for simple
same-process demos/tests. The full async mesh (Node) uses
`dispatch()` instead, which supports async handlers, security/auth, and
does not assume a response is available synchronously.
"""

from __future__ import annotations

import inspect
from typing import Awaitable, Callable, Dict, Optional, Union

from meshweaver.logging_config import get_logger
from meshweaver.protocol import PING, PONG, Message
from meshweaver.security import SecurityError, authenticate, validate_message

logger = get_logger("meshweaver.router")

Handler = Callable[[Message, tuple], Union[None, Awaitable[None]]]


class Router:
    """Dispatches messages to registered per-type handlers.

    `route()` — synchronous, in-process convenience matching the original
        prototype's demo usage (PING -> PONG only, no handlers needed).
    `dispatch()` — the real async path used by Node: validates, optionally
        authenticates, and calls the registered handler for the message's
        type. Errors are caught and logged; a bad handler never crashes
        the node.
    """

    def __init__(self, node_id: str, *, require_auth: bool = False, shared_secret: str = ""):
        self.node_id = node_id
        self.require_auth = require_auth
        self.shared_secret = shared_secret
        self._handlers: Dict[str, Handler] = {}

    def register_handler(self, message_type: str, handler: Handler) -> None:
        self._handlers[message_type] = handler

    def unregister_handler(self, message_type: str) -> None:
        self._handlers.pop(message_type, None)

    # -- synchronous demo path (kept for app.py compatibility) ----------

    def route(self, message: Message) -> Optional[Message]:
        """Simple synchronous routing used by the original PING/PONG demo
        and by unit tests that don't need real networking. Only PING is
        handled inline (returns a PONG); anything else falls through to a
        registered handler if one exists and is synchronous, else None.
        """
        try:
            validate_message(message)
        except SecurityError as exc:
            logger.warning("route(): rejected invalid message: %s", exc)
            return None

        if message.type == PING:
            return message.make_reply(PONG)

        handler = self._handlers.get(message.type)
        if handler is None:
            return None

        if inspect.iscoroutinefunction(handler):
            logger.warning(
                "route(): handler for %s is async; use dispatch() instead",
                message.type,
            )
            return None

        return handler(message, (None, None))

    # -- full async path used by Node/network -----------------------------

    async def dispatch(self, message: Message, addr: tuple) -> None:
        try:
            validate_message(message)
            authenticate(message, self.shared_secret or None, self.require_auth)
        except SecurityError as exc:
            logger.warning("dispatch(): rejected message from %s: %s", addr, exc)
            return

        handler = self._handlers.get(message.type)
        if handler is None:
            logger.debug("No handler registered for message type %s", message.type)
            return

        try:
            result = handler(message, addr)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            logger.error("Handler for %s raised: %s", message.type, exc)
