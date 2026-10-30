"""Tests for metrics and percentile calculations."""

from __future__ import annotations

import random

import pytest

from minilab.metrics import Metrics, percentile


def test_percentile_known_values() -> None:
    """Verify linear-interpolation percentiles on known sample vector."""
    samples = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert percentile(samples, 0.0) == 1.0
    assert percentile(samples, 25.0) == 2.0
    assert percentile(samples, 50.0) == 3.0
    assert percentile(samples, 90.0) == 4.6
    assert percentile(samples, 100.0) == 5.0

    with pytest.raises(ValueError, match="empty input"):
        percentile([], 50.0)

    with pytest.raises(ValueError, match="between 0.0 and 100.0"):
        percentile(samples, -1.0)


def test_summary_ordering() -> None:
    """Verify percentile monotonic ordering invariant p50 <= p90 <= p99 <= max."""
    rng = random.Random(42)
    metrics = Metrics()
    for _ in range(100):
        metrics.record("ack_rtt", "arm1", rng.uniform(0.001, 0.050))

    summary = metrics.summary("ack_rtt")
    assert summary["count"] == 100
    assert summary["min_ms"] <= summary["p50_ms"]
    assert summary["p50_ms"] <= summary["p90_ms"]
    assert summary["p90_ms"] <= summary["p99_ms"]
    assert summary["p99_ms"] <= summary["max_ms"]

    # Test counters
    metrics.incr("retries", 2)
    assert metrics.counters()["retries"] == 2
