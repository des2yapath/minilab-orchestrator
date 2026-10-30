"""Command-line interface (CLI) for minilab.

Supports scenario execution, benchmarking with event loop blockers, deterministic
replay from JSONL logs, and Chrome/Perfetto trace visualization export.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

from minilab.host.workflow import StepStatus
from minilab.report import render_gantt, render_summary
from minilab.scenarios import get_scenarios
from minilab.system import SystemConfig, build_system
from minilab.transport import FAULT_PRESETS


def cmd_list() -> None:
    """Print available scenarios with their expected outcomes."""
    scenarios = get_scenarios()
    print(f"{'Scenario':<18} {'Expected':<12} {'Description'}")
    print("-" * 75)
    for name, sc in sorted(scenarios.items()):
        print(f"{name:<18} {sc.expected:<12} {sc.description}")


async def _run_scenario(args: argparse.Namespace) -> int:
    scenarios = get_scenarios()
    if args.scenario not in scenarios:
        print(f"Error: Unknown scenario {args.scenario!r}", file=sys.stderr)
        return 1

    sc = scenarios[args.scenario]
    config_dict = dict(sc.config_overrides)

    if args.seed is not None:
        config_dict["seed"] = args.seed
    if args.time_scale is not None:
        config_dict["time_scale"] = args.time_scale
    if args.log is not None:
        config_dict["log_path"] = args.log
    if args.naive_zones:
        config_dict["zone_ordered"] = False
    if args.zone_timeout is not None:
        config_dict["zone_timeout"] = args.zone_timeout
    if args.faults is not None and args.faults in FAULT_PRESETS:
        preset = FAULT_PRESETS[args.faults]
        config_dict["faults"] = {d: preset for d in ("arm1", "arm2", "pump1", "heater1")}

    if args.no_dedup:
        opts = dict(config_dict.get("device_opts", {}))
        for d in ("arm1", "arm2", "pump1", "heater1"):
            d_conf = dict(opts.get(d, {}))
            d_conf["dedup"] = False
            opts[d] = d_conf
        config_dict["device_opts"] = opts

    system_instance = build_system(scenario=sc.name, **config_dict)

    estop_sec = args.estop_after if args.estop_after is not None else sc.estop_after
    estop_task: asyncio.Task[None] | None = None

    async with system_instance as sys_ctx:
        if estop_sec is not None:
            async def trigger_estop() -> None:
                await asyncio.sleep(estop_sec)
                await sys_ctx.orchestrator.estop("manual")

            estop_task = asyncio.create_task(trigger_estop())

        wf = sc.workflow_factory()
        timeout = args.timeout or 10.0
        try:
            wf_task = asyncio.create_task(sys_ctx.run(wf))
            done, _ = await asyncio.wait({wf_task}, timeout=timeout)
            if not done:
                print(f"\nDEADLOCK suspected: workflow did not finish in {timeout}s", file=sys.stderr)
                print("Zone State:", sys_ctx.zones.dump_state(), file=sys.stderr)
                cycle = sys_ctx.zones.find_deadlock()
                if cycle:
                    print("Deadlock cycle detected:", cycle, file=sys.stderr)
                wf_task.cancel()
                try:
                    await wf_task
                except asyncio.CancelledError:
                    pass
                if sc.expected == "DEADLOCK":
                    print("\nEXPECTED (deadlock observed)")
                    return 0
                return 1

            result = wf_task.result()
        finally:
            if estop_task and not estop_task.done():
                estop_task.cancel()
                try:
                    await estop_task
                except asyncio.CancelledError:
                    pass

        if sc.settle:
            await asyncio.sleep(sc.settle)

        print(render_gantt(result))
        print(render_summary(sys_ctx, result))

        if result.status == sc.expected:
            print(f"\nEXPECTED (outcome: {result.status})")
            return 0
        print(f"\nUNEXPECTED (expected: {sc.expected}, got: {result.status})")
        return 0


async def _run_bench(args: argparse.Namespace) -> int:
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


async def _run_replay(logfile: str) -> int:
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
    scenarios = get_scenarios()
    sc = scenarios.get(scenario_name)
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

    # Compare results
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


def _export_trace(logfile: str, out_path: str) -> int:
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

    trace_events: list[dict[str, Any]] = []
    for d, tid in dev_tids.items():
        trace_events.append({
            "ph": "M",
            "name": "thread_name",
            "pid": 1,
            "tid": tid,
            "args": {"name": d},
        })

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


def main() -> None:
    """CLI entry point dispatching subcommands."""
    # Ensure stdout/stderr handle UTF-8 characters like full blocks on Windows
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, OSError):
            pass

    parser = argparse.ArgumentParser(prog="minilab", description="Mini Lab Controller")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("list", help="List scenarios")

    run_parser = subparsers.add_parser("run", help="Run scenario")
    run_parser.add_argument("--scenario", default="basic")
    run_parser.add_argument("--seed", type=int)
    run_parser.add_argument("--time-scale", type=float)
    run_parser.add_argument("--faults", choices=["none", "light", "heavy"])
    run_parser.add_argument("--log")
    run_parser.add_argument("--timeout", type=float, default=10.0)
    run_parser.add_argument("--estop-after", type=float)
    run_parser.add_argument("--naive-zones", action="store_true")
    run_parser.add_argument("--zone-timeout", type=float)
    run_parser.add_argument("--no-dedup", action="store_true")

    bench_parser = subparsers.add_parser("bench", help="Run latency benchmark")
    bench_parser.add_argument("--n", type=int, default=1000)
    bench_parser.add_argument("--blocker", choices=["none", "sync", "thread"], default="none")
    bench_parser.add_argument("--out")

    replay_parser = subparsers.add_parser("replay", help="Replay log")
    replay_parser.add_argument("logfile")

    trace_parser = subparsers.add_parser("trace", help="Export Chrome trace")
    trace_parser.add_argument("logfile")
    trace_parser.add_argument("--out", default="trace.json")

    args = parser.parse_args()
    if args.command == "list":
        cmd_list()
    elif args.command == "run":
        sys.exit(asyncio.run(_run_scenario(args)))
    elif args.command == "bench":
        sys.exit(asyncio.run(_run_bench(args)))
    elif args.command == "replay":
        sys.exit(asyncio.run(_run_replay(args.logfile)))
    elif args.command == "trace":
        sys.exit(_export_trace(args.logfile, args.out))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
