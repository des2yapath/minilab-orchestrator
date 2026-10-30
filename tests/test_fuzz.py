"""Fuzz testing across multiple random seeds under heavy fault injection."""

from __future__ import annotations

import asyncio

import pytest

from minilab.scenarios import basic_workflow
from minilab.system import build_system
from minilab.transport import FAULT_PRESETS
from tests.conftest import FAST, assert_no_leaked_tasks


@pytest.mark.parametrize("seed", range(10))
async def test_fuzz_heavy_faults_never_crash(seed: int) -> None:
    """Fuzz the system with heavy fault injection across multiple seeds.

    Verifies system invariants: finishes boundedly, yields valid status,
    releases all zones, raises no unhandled exceptions, and leaks zero tasks.
    """
    faults = {d: FAULT_PRESETS["heavy"] for d in ("arm1", "arm2", "pump1", "heater1")}
    params = dict(FAST)
    params["done_timeout"] = 3.0

    async with build_system(seed=seed, faults=faults, **params) as sys:
        try:
            result = await asyncio.wait_for(sys.run(basic_workflow()), timeout=15.0)
            assert result.status in {"SUCCESS", "DEGRADED", "ABORTED"}
        finally:
            assert not sys.zones.holders, f"Seed {seed} leaked zone locks: {sys.zones.holders}"

    # Verify no tasks were leaked after teardown
    assert_no_leaked_tasks()


async def test_log_has_run_config_first_and_step_events() -> None:
    """Verify log structure: run_config is the first event and steps emit start/done events."""
    async with build_system(time_scale=0.05) as sys:
        res = await sys.run(basic_workflow())
        assert res.status == "SUCCESS"

        events = sys.log.events
        assert len(events) >= 10
        assert events[0]["component"] == "run"
        assert events[0]["event"] == "run_config"

        starts = {e["step"] for e in sys.log.find(event="step_start")}
        dones = {e["step"] for e in sys.log.find(event="step_done")}
        expected_steps = {"heat", "prime", "move_a", "dispense", "home"}

        assert expected_steps.issubset(starts)
        assert expected_steps.issubset(dones)


async def test_replay_and_perfetto_trace_export(tmp_path: pytest.TempPathFactory) -> None:
    """Verify log replay and Perfetto trace export tool in automated testing."""
    import argparse
    import json

    from minilab.cli import _run_scenario
    from minilab.cli_tools import export_trace, run_replay

    log_file = str(tmp_path / "fuzz_run.jsonl")
    trace_file = str(tmp_path / "fuzz_trace.json")

    # Run scenario to generate log
    args_run = argparse.Namespace(
        scenario="basic",
        seed=42,
        time_scale=0.05,
        faults=None,
        log=log_file,
        timeout=10.0,
        estop_after=None,
        naive_zones=False,
        zone_timeout=None,
        no_dedup=False,
    )
    code_run = await _run_scenario(args_run)
    assert code_run == 0

    # Replay from log
    code_replay = await run_replay(log_file)
    assert code_replay == 0

    # Export trace
    code_trace = export_trace(log_file, trace_file)
    assert code_trace == 0

    trace_data = json.loads((tmp_path / "fuzz_trace.json").read_text(encoding="utf-8"))
    assert "traceEvents" in trace_data
    assert any(ev.get("ph") == "X" for ev in trace_data["traceEvents"])


def test_system_config_roundtrip() -> None:
    """Verify SystemConfig serialization roundtrip preserving nested structures."""
    from minilab.system import SystemConfig
    from minilab.transport import FaultConfig, LinkFaults

    cfg = SystemConfig(
        seed=7,
        time_scale=0.25,
        faults={"pump1": LinkFaults(to_device=FaultConfig(drop_first_n=2, drop_indices=(4, 5)))},
    )
    roundtripped = SystemConfig.from_dict(cfg.to_dict())
    assert roundtripped == cfg
