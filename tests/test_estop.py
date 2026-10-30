"""Tests for emergency stop (e-stop) propagation, latching, and physical safety."""

from __future__ import annotations

import asyncio

import pytest

from minilab.errors import EStopped
from minilab.scenarios import basic_workflow, critical_workflow, estop_workflow
from minilab.system import build_system


async def test_estop_mid_workflow() -> None:
    """Verify mid-workflow e-stop stops physical motion, releases zones, and resets targets."""
    async with build_system(time_scale=1.0) as sys:
        # Run workflow in background task
        wf_task = asyncio.create_task(sys.run(estop_workflow()))
        await asyncio.sleep(0.1)

        report = await sys.orchestrator.estop("test")
        res = await wf_task

        assert res.status == "ABORTED"
        assert report["unreachable"] == []
        assert report["duration"] < 0.5

        # All devices latched stopped
        for dev in sys.devices.values():
            assert dev.snapshot()["stopped"] is True

        # Arm move cancelled before completing -> position unchanged at (0, 0)
        assert sys.devices["arm1"].snapshot()["x"] == 0.0
        assert sys.devices["arm1"].snapshot()["y"] == 0.0

        # Heater target reset to ambient (safe cooling state)
        assert sys.devices["heater1"].snapshot()["target"] == 25.0

        # No zones held
        assert not sys.zones.holders


async def test_commands_rejected_until_reset() -> None:
    """Verify calls and runs are rejected with EStopped until reset() is executed."""
    async with build_system(time_scale=0.05) as sys:
        await sys.orchestrator.estop("manual")

        with pytest.raises(EStopped):
            await sys.orchestrator.call("arm1", "ping")

        with pytest.raises(EStopped):
            await sys.run(basic_workflow())

        # Reset system
        reset_report = await sys.orchestrator.reset()
        assert len(reset_report["reset"]) == 4

        # Operations should now proceed
        res = await sys.orchestrator.call("arm1", "ping")
        assert res.output == {"pong": True}


async def test_estop_with_unreachable_device() -> None:
    """Verify e-stop completes even when one device is disconnected and unreachable."""
    async with build_system(time_scale=0.05, ack_timeout=0.05) as sys:
        sys.links["pump1"].disconnect()

        report = await sys.orchestrator.estop("safety_test")
        assert "pump1" in report["unreachable"]
        assert "arm1" in report["acked"]
        assert "arm2" in report["acked"]
        assert "heater1" in report["acked"]

        # Expected bounded execution duration: (retries + 1) * ack_timeout + margin
        assert report["duration"] < 0.6


async def test_critical_failure_triggers_estop() -> None:
    """Verify failure of a critical step triggers automatic e-stop and aborts workflow."""
    async with build_system(
        time_scale=0.05,
        device_opts={"pump1": {"always_fail": True}},
    ) as sys:
        res = await sys.run(critical_workflow())
        assert res.status == "ABORTED"
        assert sys.estop_state.active is True
        for dev in sys.devices.values():
            assert dev.snapshot()["stopped"] is True


async def test_estop_is_idempotent() -> None:
    """Verify concurrent estop calls return identical reports without crashing."""
    async with build_system(time_scale=0.05) as sys:
        r1, r2 = await asyncio.gather(
            sys.orchestrator.estop("concurrent_1"),
            sys.orchestrator.estop("concurrent_2"),
        )
        assert r1 == r2
        assert sys.estop_state.active is True


async def test_pump_records_partial_volume_on_estop() -> None:
    """Verify pump accounts for partial volume dispensed before cancellation."""
    async with build_system(time_scale=1.0) as sys:
        wf_task = asyncio.create_task(sys.run(estop_workflow()))
        # Sleep enough for pump to start dispensing fluid (flow rate 10 ml/s)
        await asyncio.sleep(0.15)
        await sys.orchestrator.estop("partial_dispense_check")
        await wf_task

        total = sys.devices["pump1"].snapshot()["dispensed_total"]
        assert 0.0 < total < 30.0
