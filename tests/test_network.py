import asyncio

import pytest

from meshweaver.network import UDPTransport
from meshweaver.protocol import Message, PING, PONG


@pytest.mark.asyncio
async def test_udp_transport_sends_real_packets():
    """Proves messages travel through actual OS sockets on localhost, not
    in-process function calls.
    """
    received = []
    event = asyncio.Event()

    def on_message(message, addr):
        received.append((message, addr))
        event.set()

    server = UDPTransport("127.0.0.1", 0, on_message)
    await server.start()
    server_port = server.bound_port

    def noop(message, addr):
        pass

    client = UDPTransport("127.0.0.1", 0, noop)
    await client.start()

    msg = Message(type=PING, sender="client", receiver="server")
    await client.send(msg, ("127.0.0.1", server_port))

    await asyncio.wait_for(event.wait(), timeout=2.0)

    assert len(received) == 1
    got_msg, got_addr = received[0]
    assert got_msg.type == PING
    assert got_msg.sender == "client"
    assert got_addr[0] == "127.0.0.1"

    await server.stop()
    await client.stop()


@pytest.mark.asyncio
async def test_udp_transport_drops_malformed_packets_silently():
    received = []

    def on_message(message, addr):
        received.append(message)

    server = UDPTransport("127.0.0.1", 0, on_message)
    await server.start()
    port = server.bound_port

    # Send raw garbage bytes directly via a plain socket, bypassing Message
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(b"not-a-real-message{{{", ("127.0.0.1", port))
    sock.close()

    await asyncio.sleep(0.2)
    assert received == []  # dropped, not crashed

    await server.stop()


@pytest.mark.asyncio
async def test_full_ping_pong_over_real_sockets():
    """Two independent UDPTransports exchange PING/PONG over real UDP."""
    pong_received = asyncio.Event()

    def node_a_on_message(message, addr):
        if message.type == PONG:
            pong_received.set()

    node_a = UDPTransport("127.0.0.1", 0, node_a_on_message)
    await node_a.start()

    def node_b_on_message(message, addr):
        if message.type == PING:
            reply = message.make_reply(PONG)
            asyncio.create_task(node_b.send(reply, addr))

    node_b = UDPTransport("127.0.0.1", 0, node_b_on_message)
    await node_b.start()

    ping = Message(type=PING, sender="node_a", receiver="node_b")
    await node_a.send(ping, ("127.0.0.1", node_b.bound_port))

    await asyncio.wait_for(pong_received.wait(), timeout=2.0)
    assert pong_received.is_set()

    await node_a.stop()
    await node_b.stop()
