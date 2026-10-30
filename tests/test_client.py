"""Tests for host DeviceClient reliable command protocol, retries, and nudges."""

from __future__ import annotations

import asyncio
import time

import pytest

from minilab.errors import CommandTimeout, DeviceOffline
from minilab.system import build_system
from minilab.transport import FaultConfig, LinkFaults


async def test_retry_on_dropped_command() -> None:
    """Verify that dropped command frames trigger retries and eventually succeed."""
    faults = {"arm1": LinkFaults(to_device=FaultConfig(drop_first_n=2))}
    async with build_system(time_scale=0.05, ack_timeout=0.05, faults=faults) as sys:
        res = await sys.orchestrator.call("arm1", "ping")
        assert res.output == {"pong": True}
        assert res.attempts == 3
        assert sys.metrics.counters()["retries"] == 2


async def test_retry_on_dropped_ack_does_not_double_execute() -> None:
    """Verify that dropping ACK causes retry, but device dedup prevents duplicate execution."""
    faults = {"pump1": LinkFaults(to_host=FaultConfig(drop_first_n=1))}
    async with build_system(
        time_scale=0.2, ack_timeout=0.05, done_timeout=2.0, faults=faults
    ) as sys:
        res = await sys.orchestrator.call("pump1", "dispense", {"ml": 5.0})
        assert res.output["dispensed"] == 5.0
        # Allow any delayed background tasks to settle
        await asyncio.sleep(0.1)
        assert sys.devices["pump1"].snapshot()["dispensed_total"] == 5.0


async def test_done_implies_ack() -> None:
    """Verify that receiving DONE when ACK was dropped completes the command successfully."""
    faults = {"arm1": LinkFaults(to_host=FaultConfig(drop_first_n=1))}
    async with build_system(time_scale=0.05, ack_timeout=0.1, faults=faults) as sys:
        # ping is instant, so DONE arrives right after the dropped ACK
        res = await sys.orchestrator.call("arm1", "ping")
        assert res.output == {"pong": True}
        assert res.attempts == 1


async def test_lost_done_is_recovered_by_nudge() -> None:
    """Verify that a lost DONE response is re-requested via periodic nudge."""
    # Message #1 is ACK, message #2 is DONE
    faults = {"arm1": LinkFaults(to_host=FaultConfig(drop_indices=(2,)))}
    async with build_system(
        time_scale=0.05, ack_timeout=0.05, done_timeout=2.0, faults=faults
    ) as sys:
        # Set short resend_interval
        sys.clients["arm1"].resend_interval = 0.05
        res = await sys.orchestrator.call("arm1", "ping")
        assert res.output == {"pong": True}
        assert sys.metrics.counters().get("nudges", 0) >= 1


async def test_command_timeout_when_disconnected() -> None:
    """Verify link disconnect marks device offline and subsequent call fails fast."""
    async with build_system(time_scale=0.05, ack_timeout=0.05, max_retries=2) as sys:
        sys.links["arm1"].disconnect()

        # First call should timeout after (retries + 1) * ack_timeout
        with pytest.raises(CommandTimeout):
            await sys.orchestrator.call("arm1", "ping")

        assert sys.clients["arm1"].online is False

        # Next call must fail fast (< 20 ms) with DeviceOffline
        t0 = time.perf_counter()
        with pytest.raises(DeviceOffline):
            await sys.orchestrator.call("arm1", "ping")
        duration = time.perf_counter() - t0
        assert duration < 0.020


async def test_duplicates_are_idempotent() -> None:
    """Verify that duplicate command frames do not execute multiple operations."""
    faults = {"pump1": LinkFaults(to_device=FaultConfig(dup_prob=1.0))}
    async with build_system(
        time_scale=0.05, ack_timeout=0.05, done_timeout=2.0, faults=faults
    ) as sys:
        res = await sys.orchestrator.call("pump1", "dispense", {"ml": 5.0})
        assert res.output["dispensed"] == 5.0
        await asyncio.sleep(0.05)
        assert sys.devices["pump1"].snapshot()["dispensed_total"] == 5.0


async def test_orphan_responses_counted() -> None:
    """Verify late delayed duplicate responses increment orphan_responses counter."""
    # Deliver duplicates with a slight delay
    faults = {"arm1": LinkFaults(to_host=FaultConfig(dup_prob=1.0, delay_range=(0.02, 0.04)))}
    async with build_system(time_scale=0.05, ack_timeout=0.1, faults=faults) as sys:
        await sys.orchestrator.call("arm1", "ping")
        # Wait for the delayed duplicate response to arrive at client after call completed
        await asyncio.sleep(0.06)
        assert sys.metrics.counters().get("orphan_responses", 0) >= 1


async def test_latency_metrics_recorded() -> None:
    """Verify latency metrics are recorded for each successful call."""
    async with build_system(time_scale=0.05) as sys:
        for _ in range(50):
            await sys.orchestrator.call("arm1", "ping")

        summary = sys.metrics.summary("ack_rtt", "arm1")
        assert summary["count"] == 50
        assert summary["p50_ms"] > 0
