"""Tests for physical zone mutual exclusion manager."""

from __future__ import annotations

import asyncio

import pytest

from minilab.host.zones import ZoneManager


async def test_mutual_exclusion() -> None:
    """Verify that multiple tasks competing for Zone A never execute concurrently."""
    zm = ZoneManager(["A"])
    current_holders = 0
    max_concurrent = 0

    async def worker() -> None:
        nonlocal current_holders, max_concurrent
        async with zm.hold("worker", ["A"]):
            current_holders += 1
            max_concurrent = max(max_concurrent, current_holders)
            await asyncio.sleep(0.01)
            current_holders -= 1

    tasks = [asyncio.create_task(worker()) for _ in range(5)]
    await asyncio.gather(*tasks)
    assert max_concurrent == 1
    assert current_holders == 0
    assert not zm.holders


async def test_ordered_acquire_sorts_zones() -> None:
    """Verify ordered=True sorts zone acquisition order alphabetically."""
    zm = ZoneManager(["A", "B"], ordered=True)
    acquired = await zm.acquire("test", ["B", "A"])
    assert acquired == ["A", "B"]
    zm.release("test", acquired)


async def test_release_on_cancellation() -> None:
    """Verify locks are released cleanly when a holding or waiting task is cancelled."""
    zm = ZoneManager(["A"])

    async def holding_task() -> None:
        async with zm.hold("holder", ["A"]):
            await asyncio.sleep(10.0)

    t = asyncio.create_task(holding_task())
    await asyncio.sleep(0.01)
    assert zm.holders.get("A") == "holder"

    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    assert "A" not in zm.holders
    assert not zm.waiting


def test_find_deadlock_detects_cycle() -> None:
    """Verify wait-for graph cycle detection catches mutual circular waits."""
    zm = ZoneManager(["A", "B"])
    # Simulate deadlock state
    zm.holders["A"] = "task1"
    zm.waiting["task1"] = "B"

    zm.holders["B"] = "task2"
    zm.waiting["task2"] = "A"

    cycle = zm.find_deadlock()
    assert cycle is not None
    assert set(cycle) == {"task1", "task2"}
