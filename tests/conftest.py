"""Common fixtures and helpers for minilab test suite."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

FAST: dict[str, Any] = {
    "time_scale": 0.05,
    "ack_timeout": 0.05,
    "done_timeout": 2.0,
    "max_retries": 3,
}


def assert_no_leaked_tasks() -> None:
    """Assert that no background asyncio tasks remain running."""
    current = asyncio.current_task()
    remaining = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
    assert not remaining, f"Leaked tasks detected: {remaining}"


@pytest.fixture
def fast_params() -> dict[str, Any]:
    """Fixture providing fast timing configuration for testing."""
    return dict(FAST)
