"""
meshweaver.network
====================

Real asyncio UDP transport. No in-process shortcuts: messages travel
through actual sockets via `asyncio.DatagramProtocol`, even between two
nodes on the same machine.

Usage:

    transport = UDPTransport(host, port, on_message=handle)
    await transport.start()
    await transport.send(message, ("127.0.0.1", 5001))
    ...
    await transport.stop()

`on_message(message, addr)` is called for every successfully decoded and
security-validated inbound message. Malformed/invalid packets are logged
and dropped — they never crash the transport or propagate as exceptions
into the event loop.
"""

from __future__ import annotations

import asyncio
from typing import Callable, Optional, Tuple

from meshweaver.logging_config import get_logger
from meshweaver.protocol import Message
from meshweaver.security import SecurityError, decode_and_validate

logger = get_logger("meshweaver.network")

Address = Tuple[str, int]
MessageHandler = Callable[[Message, Address], None]


class _UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, on_message: MessageHandler):
        self._on_message = on_message
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: Address) -> None:
        try:
            message = decode_and_validate(data)
        except SecurityError as exc:
            logger.warning("Dropped invalid packet from %s: %s", addr, exc)
            return
        except Exception as exc:  # defensive: never let bad input kill the loop
            logger.error("Unexpected error decoding packet from %s: %s", addr, exc)
            return

        try:
            self._on_message(message, addr)
        except Exception as exc:
            logger.error("Handler raised while processing %s: %s", message, exc)

    def error_received(self, exc: Exception) -> None:
        logger.warning("UDP error received: %s", exc)
        # On Windows (ProactorEventLoop), WSAECONNRESET (WinError 10054) is raised when
        # an ICMP Port Unreachable packet is received after sending to an inactive port.
        # Python's _ProactorDatagramTransport._loop_reading catches this OSError and calls
        # error_received, but fails to re-arm recvfrom in the except block, causing the
        # transport to stop receiving datagrams. We re-arm reading here if still open.
        if (
            self.transport is not None
            and not self.transport.is_closing()
            and getattr(exc, "winerror", None) == 10054
        ):
            loop_reading = getattr(self.transport, "_loop_reading", None)
            if callable(loop_reading) and getattr(self.transport, "_read_fut", None) is None:
                loop_reading()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if exc:
            logger.warning("UDP connection lost: %s", exc)


class UDPTransport:
    """Thin async wrapper around asyncio's UDP endpoint."""

    def __init__(self, host: str, port: int, on_message: MessageHandler):
        self.host = host
        self.port = port
        self._on_message = on_message
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional[_UDPProtocol] = None

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._transport, self._protocol = await loop.create_datagram_endpoint(
            lambda: _UDPProtocol(self._on_message),
            local_addr=(self.host, self.port),
        )
        logger.info("UDP transport listening on %s:%s", self.host, self.port)

    async def stop(self) -> None:
        if self._transport is not None:
            self._transport.close()
            self._transport = None
            logger.info("UDP transport on %s:%s stopped", self.host, self.port)

    async def send(self, message: Message, addr: Address) -> None:
        if self._transport is None:
            raise RuntimeError("Transport is not started")
        data = message.to_bytes()
        self._transport.sendto(data, addr)

    @property
    def bound_port(self) -> int:
        if self._transport is None:
            raise RuntimeError("Transport is not started")
        return self._transport.get_extra_info("sockname")[1]
