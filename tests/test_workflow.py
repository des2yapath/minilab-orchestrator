"""Tests for workflow DAG validation and execution ordering."""

from __future__ import annotations

import pytest

from minilab.errors import WorkflowValidationError
from minilab.host.workflow import Step, StepStatus, Workflow
from minilab.scenarios import basic_workflow, contention_workflow
from minilab.system import build_system


def test_validation_errors() -> None:
    """Verify validation detects duplicates, missing targets, bad zones, and cycles."""
    devices = {"arm1", "pump1", "heater1"}
    zones = {"A", "B", "C"}

    # Duplicate IDs
    wf_dup = Workflow("dup", [
        Step("s1", "arm1", "home"),
        Step("s1", "arm1", "home"),
    ])
    with pytest.raises(WorkflowValidationError, match="Duplicate step id"):
        wf_dup.validate(devices, zones)

    # Unknown device
    wf_dev = Workflow("dev", [Step("s1", "laser1", "fire")])
    with pytest.raises(WorkflowValidationError, match="unknown device"):
        wf_dev.validate(devices, zones)

    # Unknown zone
    wf_zone = Workflow("zone", [Step("s1", "arm1", "home", zones=["Z"])])
    with pytest.raises(WorkflowValidationError, match="unknown zone"):
        wf_zone.validate(devices, zones)

    # Duplicate zones inside step
    wf_dup_zone = Workflow("dup_z", [Step("s1", "arm1", "home", zones=["A", "A"])])
    with pytest.raises(WorkflowValidationError, match="duplicate zones"):
        wf_dup_zone.validate(devices, zones)

    # Unknown dependency
    wf_dep = Workflow("dep", [Step("s1", "arm1", "home", depends_on=["missing"])])
    with pytest.raises(WorkflowValidationError, match="unknown dependency"):
        wf_dep.validate(devices, zones)

    # Dependency cycle
    wf_cycle = Workflow("cycle", [
        Step("s1", "arm1", "home", depends_on=["s2"]),
        Step("s2", "arm1", "home", depends_on=["s1"]),
    ])
    with pytest.raises(WorkflowValidationError, match="cycle"):
        wf_cycle.validate(devices, zones)


async def test_basic_workflow_success() -> None:
    """Verify end-to-end execution of basic happy path workflow."""
    async with build_system(time_scale=0.05) as sys:
        result = await sys.run(basic_workflow())
        assert result.status == "SUCCESS"
        for step_res in result.steps.values():
            assert step_res.status == StepStatus.DONE

        # Check physical state
        assert abs(sys.devices["heater1"].snapshot()["temp"] - 40.0) <= 0.5
        assert sys.devices["pump1"].snapshot()["dispensed_total"] == 5.0
        assert sys.devices["arm1"].snapshot()["x"] == 0.0
        assert sys.devices["arm1"].snapshot()["y"] == 0.0


async def test_parallel_steps_overlap() -> None:
    """Verify that independent initial steps (heat and prime) execute concurrently."""
    async with build_system(time_scale=0.1) as sys:
        res = await sys.run(basic_workflow())
        assert res.status == "SUCCESS"

        heat = res.steps["heat"]
        prime = res.steps["prime"]
        assert heat.started_at is not None and heat.finished_at is not None
        assert prime.started_at is not None and prime.finished_at is not None

        # Interval overlap: a.start < b.end and b.start < a.end
        overlap = (heat.started_at < prime.finished_at) and (prime.started_at < heat.finished_at)
        assert overlap, f"Expected heat ({heat.started_at}-{heat.finished_at}) to overlap prime ({prime.started_at}-{prime.finished_at})"


async def test_dependency_order() -> None:
    """Verify dependent step starts only after its dependencies have completed."""
    async with build_system(time_scale=0.05) as sys:
        res = await sys.run(basic_workflow())
        move_a = res.steps["move_a"]
        heat = res.steps["heat"]
        prime = res.steps["prime"]

        assert move_a.started_at is not None
        assert heat.finished_at is not None
        assert prime.finished_at is not None
        assert move_a.started_at >= max(heat.finished_at, prime.finished_at)


async def test_zone_contention_serialises() -> None:
    """Verify contending steps serialise access so zone hold intervals do not overlap."""
    async with build_system(time_scale=0.1) as sys:
        res = await sys.run(contention_workflow())
        assert res.status == "SUCCESS"

        s1 = res.steps["arm1_move"]
        s2 = res.steps["arm2_move"]
        assert s1.started_at is not None and s1.finished_at is not None
        assert s2.started_at is not None and s2.finished_at is not None

        # Must not overlap
        no_overlap = (s1.finished_at <= s2.started_at) or (s2.finished_at <= s1.started_at)
        assert no_overlap, f"Contention intervals overlapped: s1=({s1.started_at}-{s1.finished_at}), s2=({s2.started_at}-{s2.finished_at})"


async def test_device_error_fails_step() -> None:
    """Verify device-reported error fails step and degrades workflow without raising exception."""
    bad_wf = Workflow("bad", [
        Step("bad_dispense", "pump1", "dispense", params={"ml": 1000.0}),
    ])
    async with build_system(time_scale=0.05) as sys:
        res = await sys.run(bad_wf)
        assert res.status == "DEGRADED"
        assert res.steps["bad_dispense"].status == StepStatus.FAILED
        assert "OUT_OF_RANGE" in res.steps["bad_dispense"].error

