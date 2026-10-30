"""Physical zone mutual exclusion manager.

Provides exclusive access locking for physical spatial zones shared by robotic
actuators. Supports ordered global acquisition to prevent deadlocks, wait-for-graph
cycle analysis, and acquisition timeouts.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from minilab.errors import ZoneTimeout
from minilab.logs import EventLog


class ZoneManager:
    """Manages exclusive locks and contention state for physical spatial zones."""

    def __init__(
        self,
        names: Sequence[str],
        *,
        ordered: bool = True,
        step_delay: float = 0.0,
        log: EventLog | None = None,
    ) -> None:
        self._locks = {z: asyncio.Lock() for z in names}
        self.ordered = ordered
        self.step_delay = step_delay
        self.holders: dict[str, str] = {}
        self.waiting: dict[str, str] = {}
        self.names = tuple(names)
        self._log = log

    async def acquire(
        self, owner: str, zones: Sequence[str], timeout: float | None = None
    ) -> list[str]:
        """Acquire locks for all specified zones, releasing on any failure or cancellation."""
        order = sorted(set(zones)) if self.ordered else list(zones)
        got: list[str] = []
        try:
            for z in order:
                self.waiting[owner] = z
                if self._log:
                    self._log.emit("zones", "zone_wait", owner=owner, zone=z)
                try:
                    async with asyncio.timeout(timeout):
                        await self._locks[z].acquire()
                except TimeoutError as e:
                    holder = self.holders.get(z)
                    if self._log:
                        self._log.emit(
                            "zones", "zone_timeout", owner=owner, zone=z, holder=holder
                        )
                    raise ZoneTimeout(
                        f"{owner} timed out waiting for zone {z} held by {holder}"
                    ) from e
                finally:
                    self.waiting.pop(owner, None)

                got.append(z)
                self.holders[z] = owner
                if self._log:
                    self._log.emit("zones", "zone_acquired", owner=owner, zone=z)
                if self.step_delay > 0:
                    await asyncio.sleep(self.step_delay)
        except BaseException:
            self.release(owner, got)
            raise
        return got

    def release(self, owner: str, zones: Sequence[str]) -> None:
        """Release previously acquired zones held by owner."""
        for z in zones:
            if self.holders.get(z) == owner:
                del self.holders[z]
                self._locks[z].release()
                if self._log:
                    self._log.emit("zones", "zone_released", owner=owner, zone=z)

    @asynccontextmanager
    async def hold(
        self, owner: str, zones: Sequence[str], timeout: float | None = None
    ) -> AsyncIterator[list[str]]:
        """Context manager yielding acquired zone locks and guaranteeing release."""
        got = await self.acquire(owner, zones, timeout)
        try:
            yield got
        finally:
            self.release(owner, got)

    def dump_state(self) -> dict[str, Any]:
        """Return diagnostic dictionary of current lock holders and waiters."""
        return {
            "holders": dict(self.holders),
            "waiting": dict(self.waiting),
        }

    def find_deadlock(self) -> list[str] | None:
        """Detect circular wait conditions in the wait-for graph.

        Returns the list of owners forming the cycle if detected, or None.
        """
        for start_owner in list(self.waiting.keys()):
            visited: list[str] = [start_owner]
            curr = start_owner
            while curr in self.waiting:
                zone_needed = self.waiting[curr]
                holder = self.holders.get(zone_needed)
                if not holder:
                    break
                if holder == start_owner:
                    return visited + [holder]
                if holder in visited:
                    idx = visited.index(holder)
                    return visited[idx:] + [holder]
                visited.append(holder)
                curr = holder
        return None
