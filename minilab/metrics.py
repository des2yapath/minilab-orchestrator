"""Performance metrics collection and percentile analysis.

Measures command latency distributions (ACK round-trip and end-to-end execution)
and tracks operational counters (retries, timeouts, drops).
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any


def percentile(sorted_vals: list[float], p: float) -> float:
    """Calculate percentile using linear interpolation between closest ranks.

    Requires sorted_vals to be non-empty and pre-sorted in ascending order.
    p must be between 0 and 100 inclusive. Raises ValueError otherwise.
    """
    if not sorted_vals:
        raise ValueError("Cannot calculate percentile on empty input")
    if not (0.0 <= p <= 100.0):
        raise ValueError("Percentile p must be between 0.0 and 100.0")

    k = (len(sorted_vals) - 1) * p / 100.0
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_vals[int(k)])
    return float(sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f))


class Metrics:
    """Tracks latency samples and discrete operational counters."""

    def __init__(self) -> None:
        """Initialize empty sample buckets and zeroed counters."""
        # kind -> device -> list of seconds
        self._samples: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        self._counters: dict[str, int] = defaultdict(int)

    def record(self, kind: str, device: str, seconds: float) -> None:
        """Record a latency measurement in seconds."""
        self._samples[kind][device].append(seconds)

    def incr(self, name: str, n: int = 1) -> None:
        """Increment a named counter by n."""
        self._counters[name] += n

    def counters(self) -> dict[str, int]:
        """Return a snapshot copy of current counters."""
        return dict(self._counters)

    def summary(self, kind: str, device: str | None = None) -> dict[str, float | int]:
        """Compute statistical summary in milliseconds for a kind and optional device filter.

        Returns a dictionary with keys: count, min_ms, mean_ms, p50_ms, p90_ms, p99_ms, max_ms.
        Returns an empty dictionary if no samples exist.
        """
        vals: list[float] = []
        if kind in self._samples:
            if device is not None:
                vals.extend(self._samples[kind].get(device, []))
            else:
                for dev_samples in self._samples[kind].values():
                    vals.extend(dev_samples)

        if not vals:
            return {}

        ms_vals = sorted(v * 1000.0 for v in vals)
        count = len(ms_vals)
        mean_ms = sum(ms_vals) / count

        return {
            "count": count,
            "min_ms": round(ms_vals[0], 4),
            "mean_ms": round(mean_ms, 4),
            "p50_ms": round(percentile(ms_vals, 50.0), 4),
            "p90_ms": round(percentile(ms_vals, 90.0), 4),
            "p99_ms": round(percentile(ms_vals, 99.0), 4),
            "max_ms": round(ms_vals[-1], 4),
        }

    def dump(self, path: str) -> None:
        """Dump all samples and counters to a JSON file."""
        data: dict[str, Any] = {
            "counters": dict(self._counters),
            "samples": {
                kind: {dev: list(vals) for dev, vals in dev_map.items()}
                for kind, dev_map in self._samples.items()
            },
        }
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
