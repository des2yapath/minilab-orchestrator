"""Fluidic pump simulator.

Implements priming, volume dispensing with simulated fluid flow rate,
and precise partial volume accounting on mid-operation cancellation/e-stop.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from minilab.devices.base import Device
from minilab.errors import DeviceError


class Pump(Device):
    """Simulates a precision fluid dispensing pump."""

    fail_code: str = "OCCLUSION"

    def __init__(self, name: str, endpoint: Any, **kw: Any) -> None:
        super().__init__(name, endpoint, **kw)
        self.primed: bool = False
        self.dispensed_total: float = 0.0

    async def handle(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        """Handle pump commands: prime, dispense."""
        if action == "prime":
            return await self._prime(params.get("ml", 1.0))
        if action == "dispense":
            return await self._dispense(params.get("ml"))
        return await super().handle(action, params)

    async def _prime(self, ml: Any) -> dict[str, Any]:
        if not isinstance(ml, (int, float)) or isinstance(ml, bool):
            raise DeviceError("BAD_PARAMS")
        await self.sleep(0.2)
        self.primed = True
        return {"primed": True}

    async def _dispense(self, ml: Any) -> dict[str, Any]:
        if not isinstance(ml, (int, float)) or isinstance(ml, bool):
            raise DeviceError("BAD_PARAMS")

        fml = float(ml)
        if not (0.0 < fml <= 50.0):
            raise DeviceError("OUT_OF_RANGE")

        duration = (fml / 10.0) + self._rng.uniform(0.0, 0.02)
        t_start = time.perf_counter()
        try:
            await self.sleep(duration)
            self.dispensed_total += fml
            return {"dispensed": fml, "total": round(self.dispensed_total, 4)}
        except asyncio.CancelledError:
            elapsed_real = time.perf_counter() - t_start
            elapsed_sim = elapsed_real / self.time_scale if self.time_scale > 0 else 0.0
            partial = min(fml, elapsed_sim * 10.0)
            self.dispensed_total += round(partial, 4)
            raise

    def snapshot(self) -> dict[str, Any]:
        """Return pump state and cumulative dispensed volume."""
        return {
            "primed": self.primed,
            "dispensed_total": round(self.dispensed_total, 4),
            "stopped": self.stopped,
        }
