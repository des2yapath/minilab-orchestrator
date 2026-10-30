"""Central system orchestrator.

Coordinates workflow execution across devices, manages physical zone access,
handles system-wide emergency stops, and provides state recovery reset.
"""

from __future__ import annotations

import asyncio
from typing import Any

from minilab.host.client import CallResult, DeviceClient, EStopState
from minilab.logs import EventLog
from minilab.metrics import Metrics


class Orchestrator:
    """Manages active device clients, zone mutual exclusion, and safety state."""

    def __init__(
        self,
        clients: dict[str, DeviceClient],
        zones: Any,
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

    def register_step_tasks(self, tasks: list[asyncio.Task[Any]]) -> None:
        """Register active step tasks for cancellation tracking."""
        self._step_tasks.update(tasks)

    def unregister_step_tasks(self, tasks: list[asyncio.Task[Any]]) -> None:
        """Unregister completed step tasks."""
        self._step_tasks.difference_update(tasks)

    async def call(
        self, device: str, action: str, params: dict[str, Any] | None = None, **kw: Any
    ) -> CallResult:
        """Directly invoke a command on a specific device."""
        return await self.clients[device].call(action, params, **kw)

    async def shutdown(self) -> None:
        """Cancel and await all remaining step tasks."""
        running = [t for t in self._step_tasks if not t.done()]
        for t in running:
            t.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)
        self._step_tasks.clear()
