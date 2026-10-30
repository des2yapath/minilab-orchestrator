"""Tests for graceful degradation and offline device handling."""

from __future__ import annotations

import time

from minilab.host.workflow import Step, StepStatus, Workflow
from minilab.scenarios import degraded_workflow
from minilab.system import build_system


async def test_graceful_degradation() -> None:
    """Verify that device failure skips dependent steps while independent steps succeed."""
    async with build_system(
        time_scale=0.05,
        device_opts={"pump1": {"always_fail": True}},
    ) as sys:
        res = await sys.run(degraded_workflow())
        assert res.status == "DEGRADED"

        assert res.steps["heat"].status == StepStatus.DONE
        assert res.steps["prime"].status == StepStatus.FAILED
        assert res.steps["move_a"].status == StepStatus.SKIPPED
        assert res.steps["dispense"].status == StepStatus.SKIPPED
        assert res.steps["other_arm"].status == StepStatus.DONE


async def test_offline_device_fails_fast() -> None:
    """Verify offline device fails fast without re-running full retry timeouts."""
    wf_single = Workflow("single", [Step("p1", "pump1", "prime")])
    async with build_system(time_scale=0.05, ack_timeout=0.05, max_retries=2) as sys:
        # Disconnect pump1 to cause timeout and mark offline
        sys.links["pump1"].disconnect()
        res1 = await sys.run(wf_single)
        assert res1.status == "DEGRADED"
        assert sys.clients["pump1"].online is False

        # Second workflow targeting pump1 must fail fast (< 30ms)
        t0 = time.perf_counter()
        res2 = await sys.run(wf_single)
        elapsed = time.perf_counter() - t0

        assert res2.status == "DEGRADED"
        assert res2.steps["p1"].status == StepStatus.FAILED
        assert "DeviceOffline" in res2.steps["p1"].error
        assert elapsed < 0.030


async def test_reset_brings_device_back() -> None:
    """Verify reset restores online status and re-enables command execution."""
    wf_single = Workflow("single", [Step("p1", "pump1", "prime")])
    async with build_system(time_scale=0.05, ack_timeout=0.05, max_retries=1) as sys:
        sys.links["pump1"].disconnect()
        await sys.run(wf_single)
        assert sys.clients["pump1"].online is False

        # Reconnect physical link and reset system
        sys.links["pump1"].reconnect()
        reset_report = await sys.orchestrator.reset()
        assert "pump1" in reset_report["reset"]
        assert sys.clients["pump1"].online is True

        # Now calls should succeed
        res = await sys.orchestrator.call("pump1", "ping")
        assert res.output == {"pong": True}

