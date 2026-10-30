"""Tests for individual device simulators."""

from __future__ import annotations

import asyncio

import pytest

from minilab.devices.pump import Pump
from minilab.errors import DeviceError
from minilab.logs import EventLog
from minilab.messages import Message
from minilab.system import build_system
from minilab.transport import Link, LinkFaults


async def test_ping_and_status() -> None:
    """Verify standard instant actions (ping, status) across devices."""
    async with build_system(time_scale=0.05) as sys:
        res = await sys.orchestrator.call("arm1", "ping")
        assert res.output == {"pong": True}

        status = await sys.orchestrator.call("arm1", "status")
        assert status.output["x"] == 0.0
        assert status.output["y"] == 0.0
        assert status.output["stopped"] is False


async def test_arm_move_updates_position_only_on_completion() -> None:
    """Verify that a cancelled arm motion does not update its recorded position."""
    async with build_system(time_scale=1.0) as sys:
        arm = sys.devices["arm1"]
        # Start a long move
        task = asyncio.create_task(
            sys.orchestrator.call("arm1", "move_to", {"x": 50.0, "y": 50.0})
        )
        # Wait a tiny bit into the move, then cancel the task
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # Arm position must still be at (0, 0)
        assert arm.snapshot()["x"] == 0.0
        assert arm.snapshot()["y"] == 0.0


async def test_param_validation() -> None:
    """Verify input validation and error code propagation."""
    async with build_system(time_scale=0.05) as sys:
        # Arm out of range
        with pytest.raises(DeviceError, match="OUT_OF_RANGE"):
            await sys.orchestrator.call("arm1", "move_to", {"x": 1000.0, "y": 0.0})

        # Pump bad parameters
        with pytest.raises(DeviceError, match="BAD_PARAMS"):
            await sys.orchestrator.call("pump1", "dispense", {"ml": "a"})

        # Heater out of range
        with pytest.raises(DeviceError, match="OUT_OF_RANGE"):
            await sys.orchestrator.call("heater1", "set_temp", {"target": 500.0})

        # Unknown action
        with pytest.raises(DeviceError, match="UNKNOWN_ACTION"):
            await sys.orchestrator.call("pump1", "shake")


async def test_heater_reaches_target() -> None:
    """Verify heater temperature regulation reaches within 0.5 of target."""
    async with build_system(time_scale=0.05) as sys:
        res = await sys.orchestrator.call("heater1", "set_temp", {"target": 40.0})
        assert abs(res.output["temp"] - 40.0) <= 0.5


async def test_device_dedup() -> None:
    """Verify device-side deduplication executes command only once."""
    log = EventLog()
    link = Link("pump1", LinkFaults(), seed=42, log=log)
    pump = Pump("pump1", link.device, seed=42, time_scale=0.05, log=log)
    await pump.start()
    try:
        cmd = Message("cmd", msg_id=42, device="pump1", action="dispense", params={"ml": 5.0})
        # Send identical frame directly twice
        await link.host.send(cmd)
        ack1 = await link.host.recv()
        assert ack1.type == "ack"
        done1 = await link.host.recv()
        assert done1.type == "done"

        await link.host.send(cmd)
        ack2 = await link.host.recv()
        assert ack2.type == "ack"
        done2 = await link.host.recv()
        assert done2.type == "done"

        # Dispensed total should be 5.0, not 10.0
        assert pump.snapshot()["dispensed_total"] == 5.0
    finally:
        await pump.stop()
        await link.close()
