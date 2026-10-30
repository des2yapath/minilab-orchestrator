"""Thermal block simulator with continuous thermal physics model.

Models heat transfer using first-order differential relaxation toward target
temperature with simulated sensor noise and safe-state cooling on e-stop.
"""

from __future__ import annotations

import asyncio
from typing import Any

from minilab.devices.base import Device
from minilab.errors import DeviceError


class Heater(Device):
    """Simulates a heated reaction block with active thermal dynamics."""

    AMBIENT: float = 25.0
    fail_code: str = "SENSOR_FAULT"
    instant_actions: frozenset[str] = Device.instant_actions | frozenset({"get_temp"})

    def __init__(self, name: str, endpoint: Any, **kw: Any) -> None:
        super().__init__(name, endpoint, **kw)
        self.temp: float = self.AMBIENT
        self.target: float = self.AMBIENT
        self._thermal_task: asyncio.Task[None] | None = None

    async def on_start(self) -> None:
        """Start continuous physics simulation loop."""
        self._thermal_task = asyncio.create_task(
            self._thermal_loop(), name=f"heater_thermal:{self.name}"
        )

    async def on_stop(self) -> None:
        """Stop continuous physics simulation loop."""
        if self._thermal_task:
            self._thermal_task.cancel()
            try:
                await self._thermal_task
            except asyncio.CancelledError:
                pass
            self._thermal_task = None

    async def on_estop(self) -> None:
        """Set heating element target back to ambient temperature (safe state)."""
        self.target = self.AMBIENT

    async def _thermal_loop(self) -> None:
        """Continuous physical heating/cooling loop (runs regardless of e-stop)."""
        while True:
            await asyncio.sleep(0.02 * self.time_scale)
            self.temp += (self.target - self.temp) * 0.1

    async def handle(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        """Handle heater commands: get_temp, set_temp."""
        if action == "get_temp":
            noisy = self.temp + self._rng.gauss(0.0, 0.05)
            return {"temp": round(noisy, 2)}
        if action == "set_temp":
            return await self._set_temp(params.get("target"))
        return await super().handle(action, params)

    async def _set_temp(self, target: Any) -> dict[str, Any]:
        if not isinstance(target, (int, float)) or isinstance(target, bool):
            raise DeviceError("BAD_PARAMS")

        ftarget = float(target)
        if not (25.0 <= ftarget <= 80.0):
            raise DeviceError("OUT_OF_RANGE")

        self.target = ftarget
        sim_elapsed = 0.0
        while abs(self.temp - self.target) > 0.5:
            await self.sleep(0.02)
            sim_elapsed += 0.02
            if sim_elapsed > 20.0:
                raise DeviceError("TIMEOUT_HEATING")

        return {"temp": round(self.temp, 2)}

    def snapshot(self) -> dict[str, Any]:
        """Return temperature readings and current target."""
        return {
            "temp": round(self.temp, 2),
            "target": round(self.target, 2),
            "stopped": self.stopped,
        }

