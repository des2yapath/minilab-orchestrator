"""Host-side device client with reliable command messaging.

Implements ACK-wait with retries, DONE-wait with periodic nudging, latency
recording, deduplication tracking, and emergency-stop transmission.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterator
from typing import Any

from minilab.errors import CommandTimeout, DeviceError, DeviceOffline, EStopped
from minilab.host.client_types import CallResult, EStopState, Pending
from minilab.logs import EventLog
from minilab.messages import Message
from minilab.metrics import Metrics
from minilab.transport import Endpoint

__all__ = ["CallResult", "DeviceClient", "EStopState", "Pending"]


class DeviceClient:
    """Host controller client communicating with a single device over an Endpoint."""

    def __init__(
        self,
        name: str,
        endpoint: Endpoint,
        *,
        ids: Iterator[int],
        estop_state: EStopState,
        log: EventLog,
        metrics: Metrics,
        ack_timeout: float = 0.25,
        done_timeout: float = 5.0,
        max_retries: int = 3,
        resend_interval: float | None = None,
    ) -> None:
        self.name = name
        self.ep = endpoint
        self._ids = ids
        self.estop_state = estop_state
        self.log = log
        self.metrics = metrics
        self.ack_timeout = ack_timeout
        self.done_timeout = done_timeout
        self.max_retries = max_retries
        self.resend_interval = resend_interval if resend_interval is not None else (4 * ack_timeout)

        self.online = True
        self._pending: dict[int, Pending] = {}
        self._reader: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the background incoming message reader loop."""
        self._reader = asyncio.create_task(self._reader_loop(), name=f"client:{self.name}")

        def _on_done(t: asyncio.Task[None]) -> None:
            if not t.cancelled() and t.exception() is not None:
                self.log.emit("system", "task_crashed", task=t.get_name(), error=str(t.exception()))

        self._reader.add_done_callback(_on_done)

    async def close(self) -> None:
        """Cancel and await the reader loop."""
        if self._reader:
            self._reader.cancel()
            try:
                await self._reader
            except asyncio.CancelledError:
                pass
            self._reader = None

    async def _reader_loop(self) -> None:
        """Receive messages from endpoint and resolve pending command futures."""
        while True:
            msg = await self.ep.recv()
            p = self._pending.get(msg.msg_id)
            if p is None:
                self.log.emit(
                    f"client:{self.name}", "orphan_response", msg_id=msg.msg_id, type=msg.type
                )
                self.metrics.incr("orphan_responses")
                continue

            now = time.perf_counter()
            if msg.type == "ack":
                if not p.ack_fut.done():
                    p.t_ack = now
                    p.ack_fut.set_result(None)
                    self.log.emit(
                        f"client:{self.name}", "ack_recv", msg_id=msg.msg_id, rtt=round(now - p.t_first_send, 6)
                    )
            elif msg.type in ("done", "error"):
                if not p.ack_fut.done():
                    p.t_ack = now
                    p.ack_fut.set_result(None)
                if not p.done_fut.done():
                    p.t_done = now
                    p.done_fut.set_result(msg)
                    if msg.type == "done":
                        self.log.emit(
                            f"client:{self.name}", "done_recv", msg_id=msg.msg_id, total=round(now - p.t_first_send, 6)
                        )
                    else:
                        self.log.emit(
                            f"client:{self.name}", "error_recv", msg_id=msg.msg_id, error=msg.error
                        )

    async def _send(self, msg: Message, kind: str) -> None:
        self.log.emit(
            f"client:{self.name}", "cmd_sent", msg_id=msg.msg_id, action=msg.action, attempt=msg.attempt, kind=kind
        )
        await self.ep.send(msg)

    async def call(
        self,
        action: str,
        params: dict[str, Any] | None = None,
        *,
        ack_timeout: float | None = None,
        done_timeout: float | None = None,
        retries: int | None = None,
        force: bool = False,
    ) -> CallResult:
        """Send a command, wait for ACK (with retries), and wait for DONE (with nudges)."""
        if self.estop_state.active and not force:
            raise EStopped("e-stop active; reset required")
        if not self.online and not force:
            raise DeviceOffline(self.name)

        eff_ack_timeout = ack_timeout or self.ack_timeout
        eff_done_timeout = done_timeout or self.done_timeout
        eff_retries = self.max_retries if retries is None else retries

        msg_id = next(self._ids)
        loop = asyncio.get_running_loop()
        p = Pending(ack_fut=loop.create_future(), done_fut=loop.create_future())
        self._pending[msg_id] = p
        p.t_first_send = time.perf_counter()

        try:
            # Phase 1: Wait for ACK with retries
            attempts = 0
            for attempt in range(eff_retries + 1):
                attempts += 1
                await self._send(
                    Message(
                        "cmd", msg_id, self.name, action, params or {}, attempt=attempt
                    ),
                    kind="send" if attempt == 0 else "retry",
                )
                if attempt > 0:
                    self.metrics.incr("retries")

                await asyncio.wait({p.ack_fut}, timeout=eff_ack_timeout)
                if p.ack_fut.done():
                    break
            else:
                self.online = False
                self.metrics.incr("timeouts")
                self.log.emit(f"client:{self.name}", "cmd_timeout", msg_id=msg_id, phase="ack")
                self.log.emit(f"client:{self.name}", "device_offline", msg_id=msg_id)
                raise CommandTimeout(
                    f"{self.name}.{action}: no ACK after {eff_retries + 1} attempts"
                )

            # Phase 2: Wait for DONE with nudges
            deadline = time.perf_counter() + eff_done_timeout
            while True:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    self.online = False
                    self.metrics.incr("timeouts")
                    self.log.emit(
                        f"client:{self.name}", "cmd_timeout", msg_id=msg_id, phase="done"
                    )
                    self.log.emit(f"client:{self.name}", "device_offline", msg_id=msg_id)
                    raise CommandTimeout(
                        f"{self.name}.{action}: no DONE within {eff_done_timeout}s"
                    )

                await asyncio.wait(
                    {p.done_fut}, timeout=min(self.resend_interval, remaining)
                )
                if p.done_fut.done():
                    break

                await self._send(
                    Message(
                        "cmd", msg_id, self.name, action, params or {}, attempt=attempts
                    ),
                    kind="nudge",
                )
                self.metrics.incr("nudges")

            reply = p.done_fut.result()
            self.metrics.record("ack_rtt", self.name, p.t_ack - p.t_first_send)
            self.metrics.record("total", self.name, p.t_done - p.t_first_send)
            self.metrics.incr("calls")

            if reply.type == "error":
                raise DeviceError(reply.error)

            return CallResult(
                output=reply.result,
                attempts=attempts,
                ack_rtt=p.t_ack - p.t_first_send,
                total=p.t_done - p.t_first_send,
            )
        finally:
            self._pending.pop(msg_id, None)

    async def estop(self, retries: int = 5) -> bool:
        """Send emergency stop to device, waiting only for ACK. Works even if offline."""
        msg_id = next(self._ids)
        loop = asyncio.get_running_loop()
        p = Pending(ack_fut=loop.create_future(), done_fut=loop.create_future())
        self._pending[msg_id] = p
        p.t_first_send = time.perf_counter()

        try:
            for attempt in range(retries + 1):
                await self._send(
                    Message("estop", msg_id, self.name, attempt=attempt),
                    kind="send" if attempt == 0 else "retry",
                )
                await asyncio.wait({p.ack_fut}, timeout=self.ack_timeout)
                if p.ack_fut.done():
                    return True
            return False
        finally:
            self._pending.pop(msg_id, None)
