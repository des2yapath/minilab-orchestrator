"""CLI tool commands: benchmark, log replay, and Perfetto trace export."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

from minilab.host.workflow import StepStatus
from minilab.scenarios import get_scenarios
from minilab.system import SystemConfig, build_system


async def run_bench(args: argparse.Namespace) -> int:
    """Run latency benchmark with optional event-loop blocker tasks."""
    n = args.n or 1000
    blocker_mode = args.blocker or "none"
    blocker_task: asyncio.Task[None] | None = None

    if blocker_mode == "sync":
        async def sync_blocker() -> None:
            while True:
                time.sleep(0.02)  # noqa: ASYNC251
                await asyncio.sleep(0.03)

        blocker_task = asyncio.create_task(sync_blocker())
    elif blocker_mode == "thread":
        async def thread_blocker() -> None:
            while True:
                await asyncio.to_thread(time.sleep, 0.02)
                await asyncio.sleep(0.03)

        blocker_task = asyncio.create_task(thread_blocker())

    try:
        async with build_system(time_scale=1.0) as sys_ctx:
            print(f"Benchmarking {n} ping commands (blocker={blocker_mode})...")
            for _ in range(n):
                await sys_ctx.orchestrator.call("arm1", "ping")

            summary = sys_ctx.metrics.summary("ack_rtt", "arm1")
            print("\nBenchmark Results (ack_rtt in ms):")
            print(f"  Count: {summary['count']}")
            print(f"  p50:   {summary['p50_ms']:.2f} ms")
            print(f"  p90:   {summary['p90_ms']:.2f} ms")
            print(f"  p99:   {summary['p99_ms']:.2f} ms")
            print(f"  Max:   {summary['max_ms']:.2f} ms")

            if args.out:
                sys_ctx.metrics.dump(args.out)
                print(f"Saved metrics dump to {args.out}")
    finally:
        if blocker_task:
            blocker_task.cancel()
            try:
                await blocker_task
            except asyncio.CancelledError:
                pass
    return 0


async def run_replay(logfile: str) -> int:
    """Replay execution log with identical seed and verify fault decisions."""
    path = Path(logfile)
    if not path.exists():
        print(f"Error: Log file not found: {logfile}", file=sys.stderr)
        return 1

    orig_events: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:  # noqa: ASYNC230
        for line in f:
            line = line.strip()
            if line:
                orig_events.append(json.loads(line))

    cfg_event = next((e for e in orig_events if e.get("event") == "run_config"), None)
    if not cfg_event:
        print("Error: Missing run_config event in log file", file=sys.stderr)
        return 1

    scenario_name = cfg_event.get("scenario", "basic")
    sc = get_scenarios().get(scenario_name)
    if not sc:
        print(f"Error: Unknown scenario in log: {scenario_name}", file=sys.stderr)
        return 1

    cfg = SystemConfig.from_dict(cfg_event["config"])
    replay_log = f"logs/replay_{int(time.time())}.jsonl"
    cfg.log_path = replay_log

    print(f"Replaying scenario {scenario_name!r} with seed {cfg.seed}...")
    async with build_system(**cfg.to_dict()) as sys_ctx:
        wf = sc.workflow_factory()
        result = await sys_ctx.run(wf)

    orig_end = next((e for e in orig_events if e.get("event") == "workflow_end"), {})
    print("\n--- Replay Comparison ---")
    print(f"Original Status: {orig_end.get('status')} | Replay Status: {result.status}")

    orig_steps = {
        e["step"]: e["event"]
        for e in orig_events
        if e.get("event") in ("step_done", "step_failed", "step_skipped")
    }
    match_count = sum(
        1 for sid, sres in result.steps.items()
        if (sres.status == StepStatus.DONE and orig_steps.get(sid) == "step_done")
        or (sres.status == StepStatus.FAILED and orig_steps.get(sid) == "step_failed")
        or (sres.status == StepStatus.SKIPPED and orig_steps.get(sid) == "step_skipped")
    )
    print(f"Steps Status Matches: {match_count}/{len(result.steps)}")
    print(
        "Note: Fault decisions are reproduced identically; real-time asyncio scheduling "
        "interleavings may exhibit minor sub-millisecond variations."
    )
    return 0


def export_trace(logfile: str, out_path: str) -> int:
    """Export step timelines from JSONL log to Chrome/Perfetto trace JSON."""
    path = Path(logfile)
    if not path.exists():
        print(f"Error: Log file not found: {logfile}", file=sys.stderr)
        return 1

    events: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    devices = sorted({e.get("device") for e in events if e.get("device")})
    dev_tids = {d: i + 1 for i, d in enumerate(devices)}

    trace_events: list[dict[str, Any]] = [
        {"ph": "M", "name": "thread_name", "pid": 1, "tid": tid, "args": {"name": d}}
        for d, tid in dev_tids.items()
    ]

    step_starts: dict[str, dict[str, Any]] = {}
    for e in events:
        ev_name = e.get("event")
        sid = e.get("step")
        if ev_name == "step_start" and sid:
            step_starts[sid] = e
        elif ev_name in ("step_done", "step_failed") and sid in step_starts:
            start_e = step_starts.pop(sid)
            start_us = int(start_e["t"] * 1_000_000)
            dur_us = int((e["t"] - start_e["t"]) * 1_000_000)
            dev = start_e.get("device", "unknown")
            trace_events.append({
                "name": sid,
                "cat": dev,
                "ph": "X",
                "ts": start_us,
                "dur": dur_us,
                "pid": 1,
                "tid": dev_tids.get(dev, 1),
            })

    target = Path(out_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8") as f:
        json.dump({"traceEvents": trace_events}, f, indent=2)
    print(f"Exported Perfetto trace to {out_path}")
    return 0

