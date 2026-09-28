"""
meshweaver.cli
=================

    python -m meshweaver.cli node start --node-id node_a --host 127.0.0.1 --port 5000
    python -m meshweaver.cli node start --node-id node_b --host 127.0.0.1 --port 5001 --bootstrap 127.0.0.1:5000
    python -m meshweaver.cli ping --host 127.0.0.1 --port 5001
    python -m meshweaver.cli node status --host 127.0.0.1 --port 5000
    python -m meshweaver.cli node peers --host 127.0.0.1 --port 5000
    python -m meshweaver.cli submit --target-host 127.0.0.1 --target-port 5001 \
        --function examples.simple_task:add --args 2 3
    python -m meshweaver.cli task status --host 127.0.0.1 --port 5000 --task-id <id>
    python -m meshweaver.cli version

`node status`, `node peers`, and `task status` query a *running* node over
the network (via a STATUS message) rather than needing shared process
memory — so they can be run from a separate terminal/process, matching the
CLI's job of controlling nodes started elsewhere.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import sys
from typing import Optional

from meshweaver.config import NodeConfig
from meshweaver.node import Node

VERSION = "0.1.0"


def _parse_bootstrap(value: Optional[str]) -> list:
    if not value:
        return []
    peers = []
    for entry in value.split(","):
        host, _, port = entry.partition(":")
        peers.append((host, int(port)))
    return peers


def _load_function(dotted: str):
    """Load `module.submodule:function_name`."""
    if ":" not in dotted:
        raise ValueError("Function must be specified as 'module.path:function_name'")
    module_name, func_name = dotted.split(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, func_name)


def _coerce_arg(value: str):
    for caster in (int, float):
        try:
            return caster(value)
        except ValueError:
            continue
    return value


def _parse_arguments(args: argparse.Namespace) -> tuple[tuple, dict]:
    """Parse task arguments from CLI flags (--args, --args-json, --kwargs-json).
    Uses strict JSON parsing, never eval().
    """
    call_args = []
    call_kwargs = {}

    args_json_raw = getattr(args, "args_json", None)
    if args_json_raw is not None:
        if getattr(args, "args", None):
            raise ValueError("Cannot specify both --args and --args-json")
        try:
            parsed_args = json.loads(args_json_raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Malformed JSON for --args-json: {exc}") from exc
        if not isinstance(parsed_args, list):
            raise ValueError("--args-json must evaluate to a JSON list")
        call_args = parsed_args
    elif getattr(args, "args", None):
        call_args = [_coerce_arg(a) for a in args.args]

    kwargs_json_raw = getattr(args, "kwargs_json", None)
    if kwargs_json_raw is not None:
        try:
            parsed_kwargs = json.loads(kwargs_json_raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Malformed JSON for --kwargs-json: {exc}") from exc
        if not isinstance(parsed_kwargs, dict):
            raise ValueError("--kwargs-json must evaluate to a JSON object")
        call_kwargs = parsed_kwargs

    return tuple(call_args), call_kwargs


# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------

async def cmd_node_start(args: argparse.Namespace) -> int:
    config = NodeConfig(node_id=args.node_id, host=args.host, port=args.port)
    node = Node(config, bootstrap_peers=_parse_bootstrap(args.bootstrap))
    await node.start()
    print(f"[{args.node_id}] MeshWeaver node running on {args.host}:{args.port} (Ctrl+C to stop)")

    loop = asyncio.get_running_loop()
    try:
        import signal
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, node._stop_event.set)
    except (NotImplementedError, ImportError):
        pass  # signal handlers aren't available on all platforms

    try:
        await node._stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        print(f"\n[{args.node_id}] Shutting down...")
        await node.stop()
    return 0


async def cmd_node_stop(args: argparse.Namespace) -> int:
    config = NodeConfig(node_id="cli-stopper", host="127.0.0.1", port=0)
    node = Node(config)
    await node.start()
    try:
        ok = await node.stop_remote_node((args.host, args.port), timeout=args.timeout)
        if ok:
            print(f"Node at {args.host}:{args.port} stopped successfully.")
            return 0
        print(f"Failed to stop node at {args.host}:{args.port}: timeout or no response", file=sys.stderr)
        return 1
    finally:
        await node.stop()


async def cmd_ping(args: argparse.Namespace) -> int:
    config = NodeConfig(node_id=args.node_id or "cli-pinger", host="127.0.0.1", port=0)
    node = Node(config)
    await node.start()
    try:
        ok = await node.ping((args.host, args.port), timeout=args.timeout)
        print("PONG received — peer is reachable" if ok else "No response (timeout)")
        return 0 if ok else 1
    finally:
        await node.stop()


async def cmd_node_status(args: argparse.Namespace) -> int:
    config = NodeConfig(node_id="cli-query", host="127.0.0.1", port=0)
    node = Node(config)
    await node.start()
    try:
        status = await node.query_remote_status((args.host, args.port), timeout=args.timeout)
        if status is None:
            print(f"No response from {args.host}:{args.port} (timeout)")
            return 1
        print(f"node_id : {status.get('node_id')}")
        print(f"state   : {status.get('state')}")
        print(f"address : {status.get('host')}:{status.get('port')}")
        print(f"peers   : {len(status.get('peers', []))}")
        tasks = status.get("tasks")
        if tasks:
            print(f"tasks   : {tasks}")
        return 0
    finally:
        await node.stop()


async def cmd_node_peers(args: argparse.Namespace) -> int:
    config = NodeConfig(node_id="cli-query", host="127.0.0.1", port=0)
    node = Node(config)
    await node.start()
    try:
        status = await node.query_remote_status((args.host, args.port), timeout=args.timeout)
        if status is None:
            print(f"No response from {args.host}:{args.port} (timeout)")
            return 1
        peers = status.get("peers", [])
        if not peers:
            print("No known peers.")
        for p in peers:
            print(f"  {p['node_id']:<12} {p['host']}:{p['port']:<6} status={p['status']:<8} "
                  f"cpu={p.get('cpu')} mem={p.get('memory')}")
        return 0
    finally:
        await node.stop()


async def cmd_submit(args: argparse.Namespace) -> int:
    try:
        func = _load_function(args.function)
    except (ValueError, ImportError, AttributeError) as exc:
        print(f"Error loading function: {exc}", file=sys.stderr)
        return 2

    try:
        call_args, call_kwargs = _parse_arguments(args)
    except ValueError as exc:
        print(f"Error parsing task arguments: {exc}", file=sys.stderr)
        return 2

    config = NodeConfig(node_id="cli-submitter", host="127.0.0.1", port=0)
    node = Node(config)
    await node.start()
    try:
        target = getattr(args, "target", None)
        target_host = getattr(args, "target_host", None)
        target_port = getattr(args, "target_port", None)

        if target:
            # Explicit target mode
            thost = target_host or args.host
            tport = target_port or args.port
            if not tport:
                print("Error: --target-port or --port is required when specifying --target", file=sys.stderr)
                return 2
            node.peers.add_peer(target, thost, tport)
            result = await node.submit_task(
                func, call_args, call_kwargs, target=target, timeout=args.timeout
            )
        else:
            # Automatic scheduling mode via coordinator
            coord_host = getattr(args, "host", "127.0.0.1")
            coord_port = getattr(args, "port", None)
            if not coord_port:
                print("Error: --port or --target is required for submit", file=sys.stderr)
                return 2

            coord_status = await node.query_remote_status((coord_host, coord_port), timeout=args.timeout)
            if coord_status is None:
                print(f"Error: Could not connect to coordinator at {coord_host}:{coord_port}", file=sys.stderr)
                return 1

            peers = coord_status.get("peers", [])
            for p in peers:
                node.peers.add_peer(
                    p["node_id"], p["host"], p["port"],
                    status=p.get("status", "ALIVE"),
                    cpu=p.get("cpu"),
                    memory=p.get("memory"),
                )
            if not peers:
                coord_id = coord_status.get("node_id", "coordinator")
                node.peers.add_peer(
                    coord_id, coord_host, coord_port,
                    status="ALIVE",
                    cpu=coord_status.get("cpu"),
                    memory=coord_status.get("memory"),
                )

            result = await node.submit_task(
                func, call_args, call_kwargs, timeout=args.timeout
            )

        task_id = getattr(result, "task_id", "") or result.get("task_id", "")
        final_target = getattr(result, "target", "") or result.get("target", "") or target or "unknown"
        status_str = "COMPLETED" if result.get("status") == "success" else "FAILED"

        print(f"Task ID: {task_id}")
        print(f"Target: {final_target}")
        print(f"Status: {status_str}")
        if result.get("status") == "success":
            print(f"Result: {result.get('result')}")
            return 0
        print(f"Task failed: {result.get('message')}", file=sys.stderr)
        return 1
    finally:
        await node.stop()


async def cmd_task_status(args: argparse.Namespace) -> int:
    config = NodeConfig(node_id="cli-query", host="127.0.0.1", port=0)
    node = Node(config)
    await node.start()
    try:
        status = await node.query_remote_status((args.host, args.port), task_id=args.task_id, timeout=args.timeout)
        if status is None:
            print(f"No response from {args.host}:{args.port} (timeout)")
            return 1
        state = status.get("state", "UNKNOWN")
        result = status.get("result")
        target = status.get("target")

        print(f"task_id : {status.get('task_id')}")
        print(f"state   : {state}")
        if target:
            print(f"target  : {target}")
        if result is not None:
            print(f"result  : {result}")
        return 0
    finally:
        await node.stop()


async def cmd_task_cancel(args: argparse.Namespace) -> int:
    config = NodeConfig(node_id="cli-cancel", host="127.0.0.1", port=0)
    node = Node(config)
    await node.start()
    try:
        resp = await node.cancel_remote_task((args.host, args.port), task_id=args.task_id, timeout=args.timeout)
        if resp.get("cancelled"):
            print(f"Task {args.task_id} cancelled successfully: {resp.get('reason', '')}")
            return 0
        print(f"Failed to cancel task {args.task_id}: {resp.get('reason', 'unknown reason')}", file=sys.stderr)
        return 1
    finally:
        await node.stop()


def cmd_version(_args: argparse.Namespace) -> int:
    print(f"MeshWeaver {VERSION}")
    return 0


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="meshweaver", description="MeshWeaver P2P task broker CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    node_parser = sub.add_parser("node", help="Node lifecycle and inspection")
    node_sub = node_parser.add_subparsers(dest="node_command", required=True)

    p_start = node_sub.add_parser("start", help="Start a MeshWeaver node")
    p_start.add_argument("--node-id", required=True)
    p_start.add_argument("--host", default="127.0.0.1")
    p_start.add_argument("--port", type=int, required=True)
    p_start.add_argument("--bootstrap", default=None, help="host:port[,host:port,...] of peers to contact on startup")
    p_start.set_defaults(func=cmd_node_start)

    p_stop = node_sub.add_parser("stop", help="Gracefully stop a remote node")
    p_stop.add_argument("--host", default="127.0.0.1")
    p_stop.add_argument("--port", type=int, required=True)
    p_stop.add_argument("--timeout", type=float, default=3.0)
    p_stop.set_defaults(func=cmd_node_stop)

    p_status = node_sub.add_parser("status", help="Query a running node's status")
    p_status.add_argument("--host", default="127.0.0.1")
    p_status.add_argument("--port", type=int, required=True)
    p_status.add_argument("--timeout", type=float, default=2.0)
    p_status.set_defaults(func=cmd_node_status)

    p_peers = node_sub.add_parser("peers", help="List a running node's known peers")
    p_peers.add_argument("--host", default="127.0.0.1")
    p_peers.add_argument("--port", type=int, required=True)
    p_peers.add_argument("--timeout", type=float, default=2.0)
    p_peers.set_defaults(func=cmd_node_peers)

    p_ping = sub.add_parser("ping", help="Send a PING to a node")
    p_ping.add_argument("--node-id", default=None)
    p_ping.add_argument("--host", default="127.0.0.1")
    p_ping.add_argument("--port", type=int, required=True)
    p_ping.add_argument("--timeout", type=float, default=2.0)
    p_ping.set_defaults(func=cmd_ping)

    p_submit = sub.add_parser("submit", help="Submit a task to a target node or mesh")
    p_submit.add_argument("--target", default=None, help="node_id of the explicit target (optional)")
    p_submit.add_argument("--target-host", default=None, help="Host of explicit target")
    p_submit.add_argument("--target-port", type=int, default=None, help="Port of explicit target")
    p_submit.add_argument("--host", default="127.0.0.1", help="Coordinator host for automatic scheduling")
    p_submit.add_argument("--port", type=int, default=None, help="Coordinator port for automatic scheduling")
    p_submit.add_argument("--function", required=True, help="module.path:function_name")
    p_submit.add_argument("--args", nargs="*", default=None, help="Positional arguments (strings/numbers)")
    p_submit.add_argument("--args-json", default=None, help="JSON list of positional arguments, e.g. '[1, 2, [3, 4]]'")
    p_submit.add_argument("--kwargs-json", default=None, help="JSON object of keyword arguments, e.g. '{\"a\": 1, \"b\": 2}'")
    p_submit.add_argument("--timeout", type=float, default=10.0)
    p_submit.set_defaults(func=cmd_submit)

    p_task = sub.add_parser("task", help="Task inspection and control")
    p_task_sub = p_task.add_subparsers(dest="task_command", required=True)

    p_task_status = p_task_sub.add_parser("status", help="Query a task's status on a node")
    p_task_status.add_argument("--host", default="127.0.0.1")
    p_task_status.add_argument("--port", type=int, required=True)
    p_task_status.add_argument("--task-id", required=True)
    p_task_status.add_argument("--timeout", type=float, default=2.0)
    p_task_status.set_defaults(func=cmd_task_status)

    p_task_cancel = p_task_sub.add_parser("cancel", help="Cancel a task on a node")
    p_task_cancel.add_argument("--host", default="127.0.0.1")
    p_task_cancel.add_argument("--port", type=int, required=True)
    p_task_cancel.add_argument("--task-id", required=True)
    p_task_cancel.add_argument("--timeout", type=float, default=3.0)
    p_task_cancel.set_defaults(func=cmd_task_cancel)

    p_version = sub.add_parser("version", help="Show version")
    p_version.set_defaults(func=cmd_version)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 2

    result = args.func(args)
    if asyncio.iscoroutine(result):
        try:
            return asyncio.run(result)
        except KeyboardInterrupt:
            return 130
    return result


if __name__ == "__main__":
    sys.exit(main())
