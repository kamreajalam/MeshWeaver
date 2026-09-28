import sys

from meshweaver.protocol import Message, PING
from meshweaver.router import Router

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def run_ping_demo():
    """Run a local PING/PONG router demonstration."""

    node_a = "node_a"
    node_b = "node_b"

    router = Router(node_b)

    ping = Message(
        type=PING,
        sender=node_a,
        receiver=node_b,
    )

    print("=" * 50)
    print("MeshWeaver - Week 1 & 2 Demo: PING/PONG Router")
    print("=" * 50)

    print(f"\n[{node_a}] Sending PING...")
    print(ping.to_dict())

    response = router.route(ping)

    if response:
        print(f"\n[{node_b}] Sending response...")
        print(response.to_dict())

        if response.type == "PONG":
            print("\n[OK] PING -> PONG successful!")
    else:
        print("\n[FAILED] No response received.")


def main():
    run_ping_demo()


if __name__ == "__main__":
    main()
