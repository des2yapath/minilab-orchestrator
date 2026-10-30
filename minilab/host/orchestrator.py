"""Central system orchestrator.

Coordinates workflow execution across devices, manages physical zone access,
handles system-wide emergency stops, and provides state recovery reset.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from minilab.errors import EStopped
from minilab.host.client import CallResult, DeviceClient, EStopState
from minilab.host.runner import WorkflowRunner
from minilab.host.zones import ZoneManager
from minilab.logs import EventLog
from minilab.metrics import Metrics

if TYPE_CHECKING:
    from minilab.host.workflow import Workflow, WorkflowResult


class Orchestrator:
    """Manages active device clients, zone mutual exclusion, and safety state."""

    def __init__(
        self,
        clients: dict[str, DeviceClient],
        zones: ZoneManager,
        estop_state: EStopState,
        log: EventLog,
        metrics: Metrics,
        zone_timeout: float | None = None,
    ) -> None:
        self.clients = clients
        self.zones = zones
        self.estop_state = estop_state
        self.log = log
        self.metrics = metrics
        self.zone_timeout = zone_timeout
        self._step_tasks: set[asyncio.Task[Any]] = set()
        self._last_report: dict[str, Any] = {}
        self._estop_fut: asyncio.Future[dict[str, Any]] | None = None

    def register_step_tasks(self, tasks: list[asyncio.Task[Any]]) -> None:
        """Register active step tasks for cancellation tracking."""
        self._step_tasks.update(tasks)

    def unregister_step_tasks(self, tasks: list[asyncio.Task[Any]]) -> None:
        """Unregister completed step tasks."""
        self._step_tasks.difference_update(tasks)

    async def run(self, wf: Workflow) -> WorkflowResult:
        """Validate and run a workflow DAG."""
        if self.estop_state.active:
            raise EStopped("e-stop latched; call reset() first")
        wf.validate(set(self.clients), set(self.zones.names))
        return await WorkflowRunner(self, wf).run()

    async def call(
        self, device: str, action: str, params: dict[str, Any] | None = None, **kw: Any
    ) -> CallResult:
        """Directly invoke a command on a specific device."""
        return await self.clients[device].call(action, params, **kw)

    async def estop(self, reason: str) -> dict[str, Any]:
        """Trigger emergency stop across all devices and cancel running steps."""
        if self._estop_fut is not None:
            return await self._estop_fut

        loop = asyncio.get_running_loop()
        self._estop_fut = loop.create_future()
        self.estop_state.active = True
        t0 = time.perf_counter()
        self.log.emit("orchestrator", "estop_start", reason=reason)

        try:
            # 1. Send e-stop to all devices concurrently
            stop_tasks = [asyncio.create_task(c.estop()) for c in self.clients.values()]

            # 2. Cancel running software step tasks (except current caller task)
            me = asyncio.current_task()
            victims = [t for t in self._step_tasks if t is not me and not t.done()]
            for t in victims:
                t.cancel()

            # 3. Wait for hardware acks and software task termination
            acks = await asyncio.gather(*stop_tasks, return_exceptions=True)
            await asyncio.gather(*victims, return_exceptions=True)

            acked: list[str] = []
            unreachable: list[str] = []
            for dev_name, ack_res in zip(self.clients.keys(), acks, strict=False):
                if ack_res is True:
                    acked.append(dev_name)
                    self.log.emit("orchestrator", "estop_device_ack", device=dev_name)
                else:
                    unreachable.append(dev_name)
                    self.log.emit("orchestrator", "estop_device_unreachable", device=dev_name)

            duration = time.perf_counter() - t0
            report = {
                "reason": reason,
                "duration": round(duration, 6),
                "acked": acked,
                "unreachable": unreachable,
            }
            self.log.emit("orchestrator", "estop_complete", **report)
            self._last_report = report
            self._estop_fut.set_result(report)
            return report
        except BaseException as e:
            if not self._estop_fut.done():
                self._estop_fut.set_exception(e)
            raise

    async def reset(self) -> dict[str, Any]:
        """Clear e-stop latch and attempt to re-initialize all devices."""
        self.estop_state.active = False
        self._estop_fut = None
        self._last_report = {}
        reset_list: list[str] = []
        still_offline: list[str] = []

        for dev_name, client in self.clients.items():
            try:
                await client.call("reset", force=True)
                client.online = True
                reset_list.append(dev_name)
            except Exception:  # noqa: BLE001
                still_offline.append(dev_name)

        report = {"reset": reset_list, "still_offline": still_offline}
        self.log.emit("orchestrator", "reset", **report)
        return report

    async def shutdown(self) -> None:
        """Cancel and await all remaining step tasks."""
        running = [t for t in self._step_tasks if not t.done()]
        for t in running:
            t.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)
        self._step_tasks.clear()
