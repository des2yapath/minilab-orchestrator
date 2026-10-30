"""Reporting, Gantt visualization, and execution summaries.

Renders ASCII Gantt charts showing concurrency and zone intervals, and tabulates
operational counters, command latency percentiles, and hardware state snapshots.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from minilab.host.workflow import WorkflowResult

if TYPE_CHECKING:
    from minilab.system import System


def render_gantt(result: WorkflowResult, width: int = 50) -> str:
    """Render an ASCII Gantt chart showing step timelines and concurrency."""
    total = max(result.duration, 0.0001)
    header = (
        f"Workflow {result.name!r}  status={result.status}  duration={result.duration:.2f}s\n"
        f"{'step':<10} {'device':<10} {'0s':<10}{' ' * (width - 15)}{f'{result.duration:.2f}s':>5}\n"
    )

    lines = [header]

    # Sort steps: started steps by started_at, unstarted/skipped steps at the end
    def sort_key(s: tuple[str, Any]) -> tuple[int, float]:
        res = s[1]
        if res.started_at is not None:
            return (0, res.started_at)
        return (1, 0.0)

    sorted_steps = sorted(result.steps.items(), key=sort_key)

    for sid, sres in sorted_steps:
        step = sres.step
        status_label = sres.status.value

        if sres.started_at is not None and sres.finished_at is not None:
            start_col = min(width - 1, max(0, round((sres.started_at / total) * width)))
            end_col = min(
                width, max(start_col + 1, round((sres.finished_at / total) * width))
            )
            bar = [" "] * width
            for i in range(start_col, end_col):
                bar[i] = "█"
            bar_str = "".join(bar)
        else:
            bar_str = " " * width

        lines.append(
            f"{sid:<10} {step.device:<10} |{bar_str}|  {status_label}"
        )

    return "\n".join(lines)


def render_summary(system: System, result: WorkflowResult) -> str:
    """Render operational metrics, latency percentiles, and device state snapshots."""
    lines = ["\n=== Execution Summary ==="]

    # Operational Counters
    counters = system.metrics.counters()
    counters_str = ", ".join(
        f"{k}={v}"
        for k, v in sorted(counters.items())
    ) or "none"
    lines.append(f"Counters: {counters_str}")

    # Latency Distributions (ms)
    lines.append("\nLatency Distributions (ms):")
    lines.append(f"{'Metric':<12} {'Device':<10} {'Count':<8} {'p50':<8} {'p90':<8} {'p99':<8} {'Max':<8}")
    lines.append("-" * 65)

    for kind in ("ack_rtt", "total"):
        summary_all = system.metrics.summary(kind)
        if summary_all:
            lines.append(
                f"{kind:<12} {'(all)':<10} {summary_all['count']:<8} "
                f"{summary_all['p50_ms']:<8.2f} {summary_all['p90_ms']:<8.2f} "
                f"{summary_all['p99_ms']:<8.2f} {summary_all['max_ms']:<8.2f}"
            )
        for dev_name in system.config.devices:
            dev_summary = system.metrics.summary(kind, dev_name)
            if dev_summary:
                lines.append(
                    f"{'':<12} {dev_name:<10} {dev_summary['count']:<8} "
                    f"{dev_summary['p50_ms']:<8.2f} {dev_summary['p90_ms']:<8.2f} "
                    f"{dev_summary['p99_ms']:<8.2f} {dev_summary['max_ms']:<8.2f}"
                )

    # Hardware Device Snapshots
    lines.append("\nHardware Device State:")
    for dev_name, dev in sorted(system.devices.items()):
        snap = dev.snapshot()
        snap_str = ", ".join(f"{k}={v}" for k, v in snap.items())
        lines.append(f"  {dev_name:<10}: {snap_str}")

    return "\n".join(lines)
