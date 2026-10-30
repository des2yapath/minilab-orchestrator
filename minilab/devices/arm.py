"""Robotic arm simulator (6-DOF planar projection).

Implements arm positioning, bounds validation, motion duration calculation,
and non-atomic cancellation semantics (cancelled moves do not update position).
"""

from __future__ import annotations

import math
from typing import Any

from minilab.devices.base import Device
from minilab.errors import DeviceError


class Arm(Device):
    """Simulates a robotic positioning arm."""

    fail_code: str = "STALL"

    def __init__(self, name: str, endpoint: Any, **kw: Any) -> None:
        super().__init__(name, endpoint, **kw)
        self.x: float = 0.0
        self.y: float = 0.0

    async def handle(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        """Handle arm-specific commands: move_to, home."""
        if action == "move_to":
            return await self._move_to(params.get("x"), params.get("y"))
        if action == "home":
            return await self._move_to(0.0, 0.0)
        return await super().handle(action, params)

    async def _move_to(self, x: Any, y: Any) -> dict[str, Any]:
        if (
            not isinstance(x, (int, float))
            or isinstance(x, bool)
            or not isinstance(y, (int, float))
            or isinstance(y, bool)
        ):
            raise DeviceError("BAD_PARAMS")

        fx, fy = float(x), float(y)
        if not (0.0 <= fx <= 100.0 and 0.0 <= fy <= 100.0):
            raise DeviceError("OUT_OF_RANGE")

        dist = math.hypot(fx - self.x, fy - self.y)
        duration = 0.1 + (dist / 150.0) + self._rng.uniform(0.0, 0.05)
        await self.sleep(duration)

        # Update position only after full motion completes
        self.x = fx
        self.y = fy
        return {"x": self.x, "y": self.y, "sim_duration": round(duration, 4)}

    def snapshot(self) -> dict[str, Any]:
        """Return current arm position and status."""
        return {"x": self.x, "y": self.y, "stopped": self.stopped}

