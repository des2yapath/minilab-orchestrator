"""Command-line interface (CLI) for minilab.

Supports scenario execution, benchmarking with event loop blockers, deterministic
replay from JSONL logs, and Chrome/Perfetto trace visualization export.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from minilab.cli_tools import export_trace, run_bench, run_replay
from minilab.report import render_gantt, render_summary
from minilab.scenarios import get_scenarios
from minilab.system import build_system
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


def main() -> None:
    """CLI entry point dispatching subcommands."""
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
        sys.exit(asyncio.run(run_bench(args)))
    elif args.command == "replay":
        sys.exit(asyncio.run(run_replay(args.logfile)))
    elif args.command == "trace":
        sys.exit(export_trace(args.logfile, args.out))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
