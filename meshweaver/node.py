
"""
meshweaver.node
==================

The Node ties every subsystem together: network transport, router, DHT
(peer discovery), peer manager, gossip, heartbeat, scheduler, and executor.
This is what `meshweaver.cli node start` actually runs.

Lifecycle: STARTING -> RUNNING -> STOPPING -> STOPPED (or FAILED on
startup error).
"""

from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Dict, Optional

from meshweaver.config import NodeConfig
from meshweaver.dht import DHT
from meshweaver.executor import Executor, TaskState
from meshweaver.gossip import GossipManager
from meshweaver.heartbeat import HeartbeatManager
from meshweaver.logging_config import (
    NODE_FAILED,
    NODE_STARTED,
    NODE_STOPPED,
    PEER_DISCOVERED,
    PING_SENT,
    PONG_RECEIVED,
    TASK_ASSIGNED,
    TASK_SUBMITTED,
    get_logger,
    log_event,
)
from meshweaver.network import UDPTransport
from meshweaver.peer import PeerManager
from meshweaver.protocol import (
    ERROR,
    GOSSIP,
    HEARTBEAT,
    HEARTBEAT_ACK,
    HELLO,
    NODE_STOP,
    NODE_STOP_ACK,
    PEER_REQUEST,
    PEER_RESPONSE,
    PING,
    PONG,
    RESULT,
    TASK,
    TASK_CANCEL,
    TASK_CANCEL_ACK,
    TASK_FAILED,
    STATUS,
    Message,
)
from meshweaver.router import Router
from meshweaver.scheduler import NoAvailableNodeError, Scheduler
from meshweaver.security import attach_signature
from meshweaver.serializer import Task

logger = get_logger("meshweaver.node")


class TaskSubmissionResult(dict):
    """Dictionary subclass containing task result that preserves dict equality
    {"status": "success", "result": ...} while exposing task_id and target.
    """

    def __init__(self, data: dict, task_id: str = "", target: str = ""):
        super().__init__(data)
        self.task_id = task_id
        self.target = target

    @property
    def status(self) -> str:
        return self.get("status", "")


class NodeState(str, Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class Node:
    def __init__(self, config: NodeConfig, bootstrap_peers: Optional[list] = None):
        self.config = config
        self.node_id = config.node_id
        self.state = NodeState.STOPPED
        self.bootstrap_peers = bootstrap_peers or []  # list of (host, port)
        self._stop_event = asyncio.Event()

        self.peers = PeerManager()
        self.dht = DHT(self.node_id, config.host, config.port, k=config.dht_k, id_bits=config.dht_id_bits)
        self.router = Router(
            self.node_id,
            require_auth=config.require_auth,
            shared_secret=config.shared_secret,
        )
        self.executor = Executor(
            max_workers=config.executor_max_workers,
            default_timeout=config.task_timeout,
        )
        self.scheduler = Scheduler(self.peers, max_retries=config.max_task_retries)

        self.transport: Optional[UDPTransport] = None
        self.gossip = GossipManager(
            self.node_id, self.peers, self._send_gossip,
            interval=config.gossip_interval, fanout=config.gossip_fanout,
        )
        self.heartbeat = HeartbeatManager(
            self.node_id, self.peers, self._send_heartbeat,
            interval=config.heartbeat_interval,
            timeout=config.heartbeat_timeout,
            failure_threshold=config.failure_threshold,
            on_peer_failed=self._on_peer_failed,
        )

        # correlation_id -> asyncio.Future, resolved when the matching
        # PONG/RESULT/HEARTBEAT_ACK/PEER_RESPONSE arrives.
        self._pending: Dict[str, asyncio.Future] = {}
        # task_id -> which local peer it was sent to, for reroute bookkeeping
        self._outstanding_tasks: Dict[str, str] = {}
        # task_id -> message_id of the active request, for fast reroute on peer death
        self._task_messages: Dict[str, str] = {}
        # task_id -> full task tracking record for genuine status reporting
        self._task_records: Dict[str, dict] = {}

        self._register_handlers()

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        self.state = NodeState.STARTING
        try:
            self.transport = UDPTransport(self.config.host, self.config.port, self._on_message)
            await self.transport.start()

            if self.bootstrap_peers:
                await asyncio.gather(
                    *(self._bootstrap_peer((h, p)) for h, p in self.bootstrap_peers),
                    return_exceptions=True,
                )

            self.gossip.start()
            self.heartbeat.start()

            self.state = NodeState.RUNNING
            log_event(logger, NODE_STARTED, node=self.node_id, host=self.config.host, port=self.config.port)
        except Exception as exc:
            self.state = NodeState.FAILED
            log_event(logger, NODE_FAILED, node=self.node_id, reason=str(exc))
            raise

    async def stop(self) -> None:
        self.state = NodeState.STOPPING
        await self.gossip.stop()
        await self.heartbeat.stop()
        self.executor.shutdown()
        if self.transport is not None:
            await self.transport.stop()
            self.transport = None
        self.state = NodeState.STOPPED
        self._stop_event.set()
        log_event(logger, NODE_STOPPED, node=self.node_id)

    async def _delayed_stop(self, delay: float = 0.05) -> None:
        await asyncio.sleep(delay)
        await self.stop()

    # -- inbound message handling --------------------------------------

    def _on_message(self, message: Message, addr: tuple) -> None:
        asyncio.create_task(self.router.dispatch(message, addr))

    def _register_handlers(self) -> None:
        self.router.register_handler(PING, self._handle_ping)
        self.router.register_handler(PONG, self._handle_pong)
        self.router.register_handler(HELLO, self._handle_hello)
        self.router.register_handler(PEER_REQUEST, self._handle_peer_request)
        self.router.register_handler(PEER_RESPONSE, self._handle_peer_response)
        self.router.register_handler(GOSSIP, self._handle_gossip)
        self.router.register_handler(HEARTBEAT, self._handle_heartbeat)
        self.router.register_handler(HEARTBEAT_ACK, self._handle_heartbeat_ack)
        self.router.register_handler(TASK, self._handle_task)
        self.router.register_handler(RESULT, self._handle_result)
        self.router.register_handler(TASK_FAILED, self._handle_task_failed)
        self.router.register_handler(TASK_CANCEL, self._handle_task_cancel)
        self.router.register_handler(TASK_CANCEL_ACK, self._handle_task_cancel_ack)
        self.router.register_handler(NODE_STOP, self._handle_node_stop)
        self.router.register_handler(NODE_STOP_ACK, self._handle_node_stop_ack)
        self.router.register_handler(ERROR, self._handle_error)
        self.router.register_handler(STATUS, self._handle_status)

    async def _handle_ping(self, message: Message, addr: tuple) -> None:
        self.peers.add_peer(message.sender, addr[0], addr[1])
        reply = message.make_reply(PONG, sender=self.node_id)
        await self._send(reply, addr)

    async def _handle_pong(self, message: Message, addr: tuple) -> None:
        log_event(logger, PONG_RECEIVED, frm=message.sender)
        self.peers.add_peer(message.sender, addr[0], addr[1])
        self._resolve_pending(message.correlation_id, message)

    async def _handle_hello(self, message: Message, addr: tuple) -> None:
        is_new = message.sender not in self.peers
        self.peers.add_peer(message.sender, addr[0], addr[1])
        self.dht.observe_peer(message.sender, addr[0], addr[1])
        if is_new:
            log_event(logger, PEER_DISCOVERED, peer=message.sender, via="HELLO")

        # Learn about any peers the sender told us about (present on both
        # the initial HELLO and the reply), so discovery propagates
        # transitively without any hardcoded topology.
        self._absorb_peer_list(message.payload.get("peers", []))

        if message.correlation_id and message.correlation_id in self._pending:
            # This HELLO is itself a reply to one we sent — resolve the
            # waiting future and stop. Replying again here would create an
            # infinite HELLO<->HELLO ping-pong.
            self._resolve_pending(message.correlation_id, message)
            return

        # Genuine incoming greeting: reply once with our own HELLO plus a
        # few known peers.
        known = [
            {"node_id": nid, "host": h, "port": p}
            for (h, p, nid) in self.dht.known_peers()
            if nid != message.sender
        ][:5]
        reply = message.make_reply(HELLO, payload={"peers": known}, sender=self.node_id)
        await self._send(reply, addr)

    def _absorb_peer_list(self, entries: list) -> None:
        for entry in entries:
            try:
                nid, h, p = entry["node_id"], entry["host"], int(entry["port"])
            except (KeyError, TypeError, ValueError):
                continue
            if nid == self.node_id or nid in self.peers:
                continue
            self.peers.add_peer(nid, h, p)
            self.dht.observe_peer(nid, h, p)
            log_event(logger, PEER_DISCOVERED, peer=nid, via="PEER_LIST")
            # Greet newly discovered peer so discovery is mutual and transitive
            asyncio.create_task(self._send_hello((h, p)))

    async def _handle_peer_request(self, message: Message, addr: tuple) -> None:
        known = [
            {"node_id": nid, "host": h, "port": p}
            for (h, p, nid) in self.dht.known_peers()
            if nid != message.sender
        ]
        reply = message.make_reply(PEER_RESPONSE, payload={"peers": known}, sender=self.node_id)
        await self._send(reply, addr)

    async def _handle_peer_response(self, message: Message, addr: tuple) -> None:
        self._absorb_peer_list(message.payload.get("peers", []))
        self._resolve_pending(message.correlation_id, message)

    async def _handle_gossip(self, message: Message, addr: tuple) -> None:
        self.gossip.handle_gossip(message.payload)

    async def _handle_heartbeat(self, message: Message, addr: tuple) -> None:
        self.peers.add_peer(message.sender, addr[0], addr[1])
        reply = message.make_reply(HEARTBEAT_ACK, sender=self.node_id)
        await self._send(reply, addr)

    async def _handle_heartbeat_ack(self, message: Message, addr: tuple) -> None:
        self.heartbeat.handle_ack(message.sender)
        self.peers.get_peer(message.sender) and self.peers.get_peer(message.sender).touch()

    async def _handle_task(self, message: Message, addr: tuple) -> None:
        try:
            task = Task.from_message_payload(message.payload)
        except Exception as exc:
            logger.error("Malformed TASK from %s: %s", message.sender, exc)
            await self._send(message.make_reply(ERROR, {"reason": "malformed_task"}, sender=self.node_id), addr)
            return

        log_event(logger, TASK_ASSIGNED, task_id=task.task_id, frm=task.sender)
        result_dict = await self.executor.execute_task(
            task.payload, task_id=task.task_id, timeout=task.timeout or self.config.task_timeout
        )
        reply_type = RESULT if result_dict["status"] == "success" else TASK_FAILED
        await self._send(message.make_reply(reply_type, payload={
            "task_id": task.task_id,
            **result_dict,
        }, sender=self.node_id), addr)

    async def _handle_result(self, message: Message, addr: tuple) -> None:
        tid = message.payload.get("task_id", "")
        self._outstanding_tasks.pop(tid, None)
        self._task_messages.pop(tid, None)
        if tid in self._task_records:
            self._task_records[tid]["state"] = "COMPLETED"
            self._task_records[tid]["result"] = message.payload.get("result")
            self._task_records[tid]["updated_at"] = time.time()
        self._resolve_pending(message.correlation_id, message)

    async def _handle_task_failed(self, message: Message, addr: tuple) -> None:
        tid = message.payload.get("task_id", "")
        self._outstanding_tasks.pop(tid, None)
        self._task_messages.pop(tid, None)
        if tid in self._task_records:
            self._task_records[tid]["state"] = "FAILED"
            self._task_records[tid]["error"] = message.payload.get("message")
            self._task_records[tid]["updated_at"] = time.time()
        self._resolve_pending(message.correlation_id, message)

    async def _handle_task_cancel(self, message: Message, addr: tuple) -> None:
        task_id = message.payload.get("task_id", "")
        # 1. Check local executor
        exec_status = self.executor.get_task_status(task_id)
        if exec_status is not None:
            if exec_status in (TaskState.COMPLETED, TaskState.FAILED, TaskState.TIMEOUT):
                reply = message.make_reply(
                    TASK_CANCEL_ACK,
                    payload={"task_id": task_id, "cancelled": False, "reason": f"Task already {exec_status.value}"},
                    sender=self.node_id,
                )
            elif exec_status == TaskState.CANCELLED:
                reply = message.make_reply(
                    TASK_CANCEL_ACK,
                    payload={"task_id": task_id, "cancelled": True, "reason": "Task already cancelled"},
                    sender=self.node_id,
                )
            else:
                ok = await self.executor.cancel_task(task_id)
                reply = message.make_reply(
                    TASK_CANCEL_ACK,
                    payload={"task_id": task_id, "cancelled": ok, "reason": "Task cancelled successfully" if ok else "Failed to cancel task"},
                    sender=self.node_id,
                )
            await self._send(reply, addr)
            return

        # 2. Check coordinated task records
        rec = self._task_records.get(task_id)
        if rec:
            if rec["state"] in ("COMPLETED", "FAILED", "CANCELLED"):
                reply = message.make_reply(
                    TASK_CANCEL_ACK,
                    payload={"task_id": task_id, "cancelled": False, "reason": f"Task already {rec['state']}"},
                    sender=self.node_id,
                )
                await self._send(reply, addr)
                return

            target_node = rec.get("target", "")
            target_peer = self.peers.get_peer(target_node)
            if target_peer:
                fwd_msg = Message(
                    type=TASK_CANCEL,
                    sender=self.node_id,
                    receiver=target_node,
                    payload={"task_id": task_id},
                )
                fwd_resp = await self._request_with_timeout(fwd_msg, target_peer.address, timeout=2.0)
                if fwd_resp and fwd_resp.payload.get("cancelled"):
                    rec["state"] = "CANCELLED"
                    reply = message.make_reply(
                        TASK_CANCEL_ACK,
                        payload={"task_id": task_id, "cancelled": True, "reason": "Task cancelled on remote worker"},
                        sender=self.node_id,
                    )
                    await self._send(reply, addr)
                    return

            rec["state"] = "CANCELLED"
            reply = message.make_reply(
                TASK_CANCEL_ACK,
                payload={"task_id": task_id, "cancelled": True, "reason": "Task marked cancelled"},
                sender=self.node_id,
            )
            await self._send(reply, addr)
            return

        # 3. Not found
        reply = message.make_reply(
            TASK_CANCEL_ACK,
            payload={"task_id": task_id, "cancelled": False, "reason": "Task not found"},
            sender=self.node_id,
        )
        await self._send(reply, addr)

    async def _handle_task_cancel_ack(self, message: Message, addr: tuple) -> None:
        self._resolve_pending(message.correlation_id, message)

    async def _handle_node_stop(self, message: Message, addr: tuple) -> None:
        log_event(logger, "NODE_STOP_REQUESTED", sender=message.sender)
        reply = message.make_reply(
            NODE_STOP_ACK,
            payload={"status": "stopping", "node_id": self.node_id},
            sender=self.node_id,
        )
        await self._send(reply, addr)
        asyncio.create_task(self._delayed_stop(0.05))

    async def _handle_node_stop_ack(self, message: Message, addr: tuple) -> None:
        self._resolve_pending(message.correlation_id, message)

    async def _handle_error(self, message: Message, addr: tuple) -> None:
        self._resolve_pending(message.correlation_id, message)
        logger.warning("Received ERROR from %s: %s", message.sender, message.payload)

    async def _handle_status(self, message: Message, addr: tuple) -> None:
        """Answers remote STATUS queries: either the node's own status, or
        a specific task_id's execution status (used by the CLI so 'node
        status', 'node peers', and 'task status' can be run against a
        running node from a separate process).
        """
        if message.correlation_id and message.correlation_id in self._pending:
            # This STATUS message is itself a reply to a query we sent —
            # resolve the waiting future and stop, rather than treating it
            # as a fresh incoming query (which would cause a STATUS<->STATUS
            # ping-pong).
            self._resolve_pending(message.correlation_id, message)
            return

        query = message.payload.get("query")
        if query == "task_status":
            task_id = message.payload.get("task_id", "")
            rec = self._task_records.get(task_id)
            if rec:
                state_str = rec["state"]
                result = rec.get("result")
                target = rec.get("target")
            else:
                exec_state = self.executor.get_task_status(task_id)
                if exec_state is not None:
                    state_str = exec_state.value
                    res_dict = self.executor.get_result(task_id)
                    result = res_dict.get("result") if res_dict else None
                    target = self.node_id
                else:
                    state_str = "UNKNOWN"
                    result = None
                    target = None
            payload = {
                "task_id": task_id,
                "state": state_str,
                "target": target,
                "result": result,
            }
        else:
            payload = self.status()
        await self._send(message.make_reply(STATUS, payload=payload, sender=self.node_id), addr)

    def _resolve_pending(self, correlation_id: Optional[str], message: Message) -> None:
        if not correlation_id:
            return
        fut = self._pending.pop(correlation_id, None)
        if fut is not None and not fut.done():
            fut.set_result(message)

    def _on_peer_failed(self, node_id: str) -> None:
        """Called by HeartbeatManager when a peer is declared DEAD. Any
        task we had outstanding on that peer gets rerouted.
        """
        affected = [tid for tid, target in self._outstanding_tasks.items() if target == node_id]
        for task_id in affected:
            msg_id = self._task_messages.get(task_id)
            if msg_id and msg_id in self._pending:
                fut = self._pending.get(msg_id)
                if fut is not None and not fut.done():
                    fut.set_result(None)
            asyncio.create_task(self._reroute_task(task_id))

    async def _reroute_task(self, task_id: str) -> None:
        new_peer = self.scheduler.reroute(task_id)
        if new_peer is None:
            logger.error("Task %s exhausted retries; giving up", task_id)
            self._outstanding_tasks.pop(task_id, None)
            return
        logger.info("Rerouting task %s to %s", task_id, new_peer.node_id)
        # Caller-facing resubmission is handled by submit_task's own retry
        # loop when it detects this; here we just update bookkeeping.
        self._outstanding_tasks[task_id] = new_peer.node_id

    # -- outbound helpers -----------------------------------------------

    async def _send(self, message: Message, addr: tuple) -> None:
        if self.config.require_auth and self.config.shared_secret:
            attach_signature(message, self.config.shared_secret)
        await self.transport.send(message, addr)

    async def _send_hello(self, addr: tuple) -> None:
        hello = Message(type=HELLO, sender=self.node_id, receiver="*")
        await self._send(hello, addr)

    async def _bootstrap_peer(self, addr: tuple) -> bool:
        """Reliably greet a bootstrap peer with retries and timeout."""
        for attempt in range(1, self.config.bootstrap_retries + 1):
            known = [
                {"node_id": nid, "host": h, "port": p}
                for (h, p, nid) in self.dht.known_peers()
            ][:5]
            hello = Message(
                type=HELLO,
                sender=self.node_id,
                receiver="*",
                payload={"peers": known},
            )
            response = await self._request_with_timeout(hello, addr, self.config.bootstrap_timeout)
            if response is not None and response.type == HELLO:
                self.peers.add_peer(response.sender, addr[0], addr[1])
                self.dht.observe_peer(response.sender, addr[0], addr[1])
                self._absorb_peer_list(response.payload.get("peers", []))
                return True
            if attempt < self.config.bootstrap_retries:
                await asyncio.sleep(self.config.bootstrap_retry_interval)
        logger.warning("Failed to bootstrap with peer %s after %d attempts", addr, self.config.bootstrap_retries)
        return False

    async def _send_gossip(self, status: dict, addr: tuple) -> None:
        msg = Message(type=GOSSIP, sender=self.node_id, receiver="*", payload=status)
        await self._send(msg, addr)

    async def _send_heartbeat(self, addr: tuple) -> None:
        msg = Message(type=HEARTBEAT, sender=self.node_id, receiver="*")
        await self._send(msg, addr)

    async def _request_with_timeout(self, message: Message, addr: tuple, timeout: float) -> Optional[Message]:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[message.message_id] = fut
        try:
            await self._send(message, addr)
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self._pending.pop(message.message_id, None)

    # -- public API used by CLI / dashboard / tests ----------------------

    async def ping(self, target_addr: tuple, timeout: Optional[float] = None) -> bool:
        msg = Message(type=PING, sender=self.node_id, receiver="*")
        log_event(logger, PING_SENT, to=target_addr)
        response = await self._request_with_timeout(msg, target_addr, timeout or self.config.request_timeout)
        return response is not None and response.type == PONG

    async def discover_peers(self, bootstrap_addr: tuple, timeout: Optional[float] = None) -> int:
        """Ask a bootstrap peer who else it knows (PEER_REQUEST/RESPONSE)."""
        msg = Message(type=PEER_REQUEST, sender=self.node_id, receiver="*")
        before = len(self.peers)
        await self._request_with_timeout(msg, bootstrap_addr, timeout or self.config.request_timeout)
        return len(self.peers) - before

    async def submit_task(self, func, args=(), kwargs=None, *, target: Optional[str] = None,
                           timeout: Optional[float] = None, task_id: Optional[str] = None) -> TaskSubmissionResult:
        """Serialize `func(*args, **kwargs)`, send it to `target` (or let
        the scheduler pick the least-loaded ALIVE peer), and await the
        RESULT/TASK_FAILED, retrying on a different peer on failure up to
        max_task_retries.
        """
        task = Task.create(
            func, args, kwargs, sender=self.node_id, target=target or "",
            timeout=timeout or self.config.task_timeout,
        )
        if task_id:
            task.task_id = task_id
        log_event(logger, TASK_SUBMITTED, task_id=task.task_id)

        self._task_records[task.task_id] = {
            "task_id": task.task_id,
            "state": "PENDING",
            "target": target or "",
            "result": None,
            "error": None,
            "submitted_at": time.time(),
            "updated_at": time.time(),
        }

        attempts = 0
        excluded: list[str] = []
        last_peer_id = ""
        while attempts <= self.config.max_task_retries:
            attempts += 1
            try:
                if target and attempts == 1:
                    peer = self.peers.get_peer(target)
                    if peer is None:
                        self._task_records[task.task_id]["state"] = "FAILED"
                        self._task_records[task.task_id]["error"] = f"Unknown target peer: {target}"
                        return TaskSubmissionResult(
                            {"status": "error", "message": f"Unknown target peer: {target}"},
                            task_id=task.task_id, target=target,
                        )
                else:
                    peer = self.scheduler.select_node(exclude=excluded)
            except NoAvailableNodeError:
                self._task_records[task.task_id]["state"] = "FAILED"
                self._task_records[task.task_id]["error"] = "No available peer to run task"
                return TaskSubmissionResult(
                    {"status": "error", "message": "No available peer to run task"},
                    task_id=task.task_id, target=last_peer_id,
                )

            last_peer_id = peer.node_id
            self._outstanding_tasks[task.task_id] = peer.node_id
            task.target = peer.node_id
            self._task_records[task.task_id]["target"] = peer.node_id
            self._task_records[task.task_id]["state"] = "RUNNING"
            self._task_records[task.task_id]["updated_at"] = time.time()

            msg = Message(
                type=TASK, sender=self.node_id, receiver=peer.node_id,
                payload=task.to_message_payload(),
            )
            self._task_messages[task.task_id] = msg.message_id
            response = await self._request_with_timeout(
                msg, peer.address, timeout or self.config.task_timeout + 1.0
            )
            self._outstanding_tasks.pop(task.task_id, None)
            self._task_messages.pop(task.task_id, None)

            if response is None:
                excluded.append(peer.node_id)
                self._task_records[task.task_id]["state"] = "RETRYING"
                log_event(logger, "TASK_RETRIED", task_id=task.task_id, reason="timeout")
                continue
            if response.type == RESULT:
                res_val = response.payload.get("result")
                self._task_records[task.task_id]["state"] = "COMPLETED"
                self._task_records[task.task_id]["result"] = res_val
                return TaskSubmissionResult(
                    {"status": "success", "result": res_val},
                    task_id=task.task_id, target=peer.node_id,
                )
            if response.type == TASK_FAILED:
                excluded.append(peer.node_id)
                self._task_records[task.task_id]["state"] = "RETRYING"
                log_event(logger, "TASK_RETRIED", task_id=task.task_id, reason="remote_failure")
                continue
            self._task_records[task.task_id]["state"] = "FAILED"
            return TaskSubmissionResult(
                {"status": "error", "message": f"Unexpected response type {response.type}"},
                task_id=task.task_id, target=peer.node_id,
            )

        self._task_records[task.task_id]["state"] = "FAILED"
        self._task_records[task.task_id]["error"] = "Task failed after exhausting retries"
        return TaskSubmissionResult(
            {"status": "error", "message": "Task failed after exhausting retries"},
            task_id=task.task_id, target=last_peer_id,
        )

    async def query_remote_status(self, addr: tuple, task_id: Optional[str] = None,
                                   timeout: Optional[float] = None) -> Optional[dict]:
        """Query another (running) node's status, or a specific task's
        status if `task_id` is given, over the network.
        """
        payload = {"query": "task_status", "task_id": task_id} if task_id else {"query": "node_status"}
        msg = Message(type=STATUS, sender=self.node_id, receiver="*", payload=payload)
        response = await self._request_with_timeout(msg, addr, timeout or self.config.request_timeout)
        return response.payload if response else None

    async def cancel_remote_task(self, addr: tuple, task_id: str, timeout: Optional[float] = None) -> dict:
        """Send a TASK_CANCEL request to a remote node and await acknowledgement."""
        msg = Message(type=TASK_CANCEL, sender=self.node_id, receiver="*", payload={"task_id": task_id})
        response = await self._request_with_timeout(msg, addr, timeout or self.config.request_timeout)
        if response and response.payload:
            return response.payload
        return {"task_id": task_id, "cancelled": False, "reason": "No response to cancellation request (timeout)"}

    async def stop_remote_node(self, addr: tuple, timeout: Optional[float] = None) -> bool:
        """Send a NODE_STOP command to a remote node."""
        msg = Message(type=NODE_STOP, sender=self.node_id, receiver="*")
        response = await self._request_with_timeout(msg, addr, timeout or self.config.request_timeout)
        return response is not None and response.type == NODE_STOP_ACK

    def status(self) -> dict:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "host": self.config.host,
            "port": self.config.port,
            "peers": [p.to_dict() for p in self.peers.get_peers()],
            "failed_peers": [p.to_dict() for p in self.peers.get_peers() if p.status == "DEAD"],
            "tasks": self.executor.get_task_stats(),
        }
