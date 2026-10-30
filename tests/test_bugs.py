"""Planted concurrency bug reproductions and regression tests."""

from __future__ import annotations

import asyncio

import pytest

from minilab.host.workflow import StepStatus
from minilab.scenarios import deadlock_workflow, dispense_workflow
from minilab.system import build_system
from minilab.transport import FaultConfig, LinkFaults


@pytest.mark.bugrepro
async def test_bug1_repro_naive_zone_order_deadlocks() -> None:
    """Repro Bug #1: Unordered lock acquisition deadlocks due to circular wait."""
    async with build_system(
        time_scale=0.05,
        zone_ordered=False,
        zone_step_delay=0.01,
    ) as sys:
        wf_task = asyncio.create_task(sys.run(deadlock_workflow()))
        done, _ = await asyncio.wait({wf_task}, timeout=0.8)
        assert not done, "Expected naive zone acquisition to deadlock"

        cycle = sys.zones.find_deadlock()
        assert cycle is not None
        assert "deadlock:step1" in cycle
        assert "deadlock:step2" in cycle

        # Clean up tasks and verify lock release
        wf_task.cancel()
        try:
            await wf_task
        except asyncio.CancelledError:
            pass

        assert not sys.zones.holders, "All zones must be released after cancellation"


async def test_bug1_fix_ordered_zones_complete() -> None:
    """Fix Bug #1 (a): Global lock ordering breaks circular wait and succeeds."""
    async with build_system(
        time_scale=0.05,
        zone_ordered=True,
    ) as sys:
        res = await asyncio.wait_for(sys.run(deadlock_workflow()), timeout=2.0)
        assert res.status == "SUCCESS"
        assert res.steps["step1"].status == StepStatus.DONE
        assert res.steps["step2"].status == StepStatus.DONE
        assert not sys.zones.holders


async def test_bug1_fix_zone_timeout_breaks_deadlock() -> None:
    """Fix Bug #1 (b): Acquisition timeout breaks circular wait as watchdog fallback."""
    async with build_system(
        time_scale=0.05,
        zone_ordered=False,
        zone_step_delay=0.01,
        zone_timeout=0.3,
    ) as sys:
        res = await asyncio.wait_for(sys.run(deadlock_workflow()), timeout=2.0)
        assert res.status == "DEGRADED"

        failed_steps = [
            s for s in res.steps.values()
            if s.status == StepStatus.FAILED and "ZoneTimeout" in s.error
        ]
        assert len(failed_steps) >= 1
        assert not sys.zones.holders, "All zones must be released after timeout"


@pytest.mark.bugrepro
async def test_bug2_repro_no_dedup_double_executes() -> None:
    """Repro Bug #2: Retry on dropped ACK without idempotency deduplication causes double dose."""
    faults = {"pump1": LinkFaults(to_host=FaultConfig(drop_first_n=1))}
    async with build_system(
        time_scale=0.2,
        ack_timeout=0.05,
        done_timeout=2.0,
        faults=faults,
        device_opts={"pump1": {"dedup": False}},
    ) as sys:
        res = await sys.run(dispense_workflow())
        assert res.status == "SUCCESS"
        # Settle to allow redundant retry command execution to finish
        await asyncio.sleep(0.4)

        # Double dose occurred! 5.0 + 5.0 = 10.0
        assert sys.devices["pump1"].snapshot()["dispensed_total"] == 10.0


async def test_bug2_fix_dedup_executes_once() -> None:
    """Fix Bug #2: Idempotent message deduplication replays cached reply without double execution."""
    faults = {"pump1": LinkFaults(to_host=FaultConfig(drop_first_n=1))}
    async with build_system(
        time_scale=0.2,
        ack_timeout=0.05,
        done_timeout=2.0,
        faults=faults,
        device_opts={"pump1": {"dedup": True}},
    ) as sys:
        res = await sys.run(dispense_workflow())
        assert res.status == "SUCCESS"
        await asyncio.sleep(0.4)

        # Executed exactly once
        assert sys.devices["pump1"].snapshot()["dispensed_total"] == 5.0
