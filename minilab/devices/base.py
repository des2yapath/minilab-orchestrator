"""Base device simulator class.

Provides common life-cycle management, command reader loop, idempotency caching,
instant action handling, and emergency stop latching for simulated hardware.
"""

from __future__ import annotations

import asyncio
import random
from typing import Any

from minilab.errors import DeviceError
from minilab.logs import EventLog
from minilab.messages import Message
from minilab.transport import Endpoint


class Device:
    """Base class for all simulated hardware devices."""

    instant_actions: frozenset[str] = frozenset({"ping", "status"})
    fail_code: str = "INJECTED_FAULT"

    def __init__(
        self,
        name: str,
        endpoint: Endpoint,
        *,
        seed: int,
        time_scale: float = 1.0,
        log: EventLog,
        dedup: bool = True,
        always_fail: bool = False,
        error_prob: float = 0.0,
    ) -> None:
        self.name = name
        self.ep = endpoint
        self.seed = seed
        self.time_scale = time_scale
        self._log = log
        self.dedup = dedup
        self.always_fail = always_fail
        self.error_prob = error_prob

        self.stopped = False
        self._replies: dict[int, Message | None] = {}
        self._exec_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[None]] = set()
        self._main: asyncio.Task[None] | None = None
        self._rng = random.Random(f"{seed}:device:{name}")

    async def start(self) -> None:
        """Start the device background tasks and command reader loop."""
        await self.on_start()
        self._log.emit(f"device:{self.name}", "device_start")
        self._main = asyncio.create_task(self._loop(), name=f"device:{self.name}")

    async def stop(self) -> None:
        """Stop reader loop and all executing tasks cleanly."""
        if self._main:
            self._main.cancel()
        for t in list(self._tasks):
            t.cancel()
        all_tasks = [t for t in [self._main, *self._tasks] if t is not None]
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        self._tasks.clear()
        await self.on_stop()
        self._log.emit(f"device:{self.name}", "device_stop")

    async def on_start(self) -> None:
        """Hook called when device starts."""

    async def on_stop(self) -> None:
        """Hook called when device stops."""

    async def on_estop(self) -> None:
        """Hook called when emergency stop is triggered."""

    def on_reset(self) -> None:
        """Hook called when device is reset."""

    async def handle(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        """Handle incoming command action. Subclasses call super() for unknown actions."""
        if action == "ping":
            return {"pong": True}
        if action == "status":
            return self.snapshot()
        raise DeviceError("UNKNOWN_ACTION")

    def snapshot(self) -> dict[str, Any]:
        """Return diagnostic state snapshot."""
        return {"stopped": self.stopped}

    async def sleep(self, sim_seconds: float) -> None:
        """Sleep for simulated seconds scaled by time_scale."""
        await asyncio.sleep(sim_seconds * self.time_scale)

    async def _loop(self) -> None:
        """Continuously receive and dispatch frames from the host."""
        while True:
            msg = await self.ep.recv()
            if msg.type == "estop":
                await self._on_estop(msg)
            elif msg.type == "cmd":
                await self._on_cmd(msg)

    async def _on_cmd(self, msg: Message) -> None:
        """Handle incoming command, applying deduplication and idempotency rules."""
        is_dup = msg.msg_id in self._replies
        self._log.emit(
            f"device:{self.name}",
            "device_cmd_recv",
            msg_id=msg.msg_id,
            action=msg.action,
            attempt=msg.attempt,
            duplicate=is_dup,
        )

        if self.dedup and is_dup:
            await self.ep.send(Message("ack", msg.msg_id, self.name))
            reply = self._replies[msg.msg_id]
            if reply is not None:
                await self.ep.send(reply)
            return

        self._replies[msg.msg_id] = None
        await self.ep.send(Message("ack", msg.msg_id, self.name))
        t = asyncio.create_task(self._execute(msg))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _execute(self, msg: Message) -> None:
        """Execute command in isolated task so reader loop stays responsive to e-stop."""
        try:
            if msg.action == "reset":
                result = self._do_reset()
            elif msg.action in self.instant_actions:
                result = await self.handle(msg.action, msg.params)
            else:
                if self.stopped:
                    raise DeviceError("ESTOPPED")
                async with self._exec_lock:
                    if self.stopped:
                        raise DeviceError("ESTOPPED")
                    self._maybe_inject_failure()
                    self._log.emit(
                        f"device:{self.name}",
                        "device_exec_start",
                        msg_id=msg.msg_id,
                        action=msg.action,
                    )
                    result = await self.handle(msg.action, msg.params)
                    self._log.emit(
                        f"device:{self.name}",
                        "device_exec_done",
                        msg_id=msg.msg_id,
                        action=msg.action,
                    )
            reply = Message("done", msg.msg_id, self.name, result=result)
        except asyncio.CancelledError:
            self._replies[msg.msg_id] = Message("error", msg.msg_id, self.name, error="ESTOPPED")
            self._log.emit(
                f"device:{self.name}",
                "device_exec_error",
                msg_id=msg.msg_id,
                action=msg.action,
                error="ESTOPPED",
            )
            raise
        except DeviceError as e:
            reply = Message("error", msg.msg_id, self.name, error=str(e))
            self._log.emit(
                f"device:{self.name}",
                "device_exec_error",
                msg_id=msg.msg_id,
                action=msg.action,
                error=str(e),
            )

        self._replies[msg.msg_id] = reply
        await self.ep.send(reply)

    async def _on_estop(self, msg: Message) -> None:
        """Immediately latch e-stop, cancel executions, and notify hardware hook."""
        self.stopped = True
        running = list(self._tasks)
        for t in running:
            t.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)
        await self.on_estop()
        self._log.emit(f"device:{self.name}", "device_estop")
        await self.ep.send(Message("ack", msg.msg_id, self.name))

    def _do_reset(self) -> dict[str, Any]:
        """Clear e-stop latch and invoke reset hook."""
        self.stopped = False
        self.on_reset()
        self._log.emit(f"device:{self.name}", "device_reset")
        return {"reset": True}

    def _maybe_inject_failure(self) -> None:
        """Inject configured synthetic failure."""
        if self.always_fail or (self.error_prob > 0.0 and self._rng.random() < self.error_prob):
            raise DeviceError(self.fail_code)
