"""
meshweaver.dashboard
=======================

A minimal, functional (not fancy) live dashboard. Polls a running node's
STATUS endpoint over the network at a fixed interval and renders it as
plain text in the terminal: node identity/state, known peers with their
CPU/RAM/heartbeat status, and a simple mesh topology tree.

Run:

    python -m meshweaver.dashboard --host 127.0.0.1 --port 5000

Deliberately terminal/text-based rather than a web UI, per the project's
"zero-dependency", "functionality over styling" guidance — no extra HTTP
server or frontend framework required.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time

from meshweaver.config import NodeConfig
from meshweaver.node import Node


import sys

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def _clear_screen() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def render(status: dict, target_addr: tuple) -> str:
    lines = []
    lines.append("=" * 60)
    lines.append(" MESHWEAVER DASHBOARD")
    lines.append("=" * 60)
    lines.append(f" Watching node: {status.get('node_id')} @ {target_addr[0]}:{target_addr[1]}")
    lines.append(f" State: {status.get('state')}")

    tasks = status.get("tasks")
    if tasks:
        lines.append("-" * 60)
        lines.append(
            f" Tasks: total={tasks.get('total', 0)} "
            f"running={tasks.get('running', 0)} "
            f"pending={tasks.get('pending', 0)} "
            f"completed={tasks.get('completed', 0)} "
            f"failed={tasks.get('failed', 0)} "
            f"cancelled={tasks.get('cancelled', 0)}"
        )

    lines.append("-" * 60)

    peers = status.get("peers", [])
    alive_peers = [p for p in peers if p.get("status") != "DEAD"]
    failed_peers = status.get("failed_peers", [p for p in peers if p.get("status") == "DEAD"])

    lines.append(f" Known peers: {len(peers)} ({len(alive_peers)} active, {len(failed_peers)} failed)")
    for p in peers:
        cpu = p.get("cpu")
        mem = p.get("memory")
        cpu_s = f"{cpu:5.1f}%" if isinstance(cpu, (int, float)) else "  n/a"
        mem_s = f"{mem:5.1f}%" if isinstance(mem, (int, float)) else "  n/a"
        lines.append(
            f"   - {p['node_id']:<12} {p['host']}:{p['port']:<6} "
            f"[{p['status']:<7}] cpu={cpu_s} mem={mem_s}"
        )

    if failed_peers:
        lines.append("-" * 60)
        lines.append(f" Failed peers ({len(failed_peers)}):")
        for fp in failed_peers:
            lines.append(f"   [!] {fp['node_id']} @ {fp['host']}:{fp['port']} (DEAD)")

    lines.append("-" * 60)
    lines.append(" Mesh topology:")
    lines.append(f"   {status.get('node_id')}")
    use_unicode = sys.stdout.encoding and "utf" in sys.stdout.encoding.lower()
    for i, p in enumerate(peers):
        if use_unicode:
            branch = "└──" if i == len(peers) - 1 else "├──"
        else:
            branch = "\\--" if i == len(peers) - 1 else "+--"
        lines.append(f"   {branch} {p['node_id']} ({p['status']})")

    lines.append("=" * 60)
    lines.append(f" Last updated: {time.strftime('%H:%M:%S')}  (Ctrl+C to exit)")
    return "\n".join(lines)


async def run_dashboard(host: str, port: int, interval: float = 2.0) -> None:
    config = NodeConfig(node_id="dashboard", host="127.0.0.1", port=0)
    watcher = Node(config)
    await watcher.start()
    try:
        while True:
            status = await watcher.query_remote_status((host, port), timeout=interval)
            _clear_screen()
            if status is None:
                print(f"No response from {host}:{port} — is the node running?")
            else:
                print(render(status, (host, port)))
            await asyncio.sleep(interval)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await watcher.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="MeshWeaver live dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()
    try:
        asyncio.run(run_dashboard(args.host, args.port, args.interval))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
