"""Data structures and state objects for host device clients."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from minilab.messages import Message


@dataclass
class EStopState:
    """Shared state container holding emergency stop latch status."""

    active: bool = False


@dataclass
class CallResult:
    """Result data from a successful device command execution."""

    output: dict[str, Any]
    attempts: int
    ack_rtt: float
    total: float


@dataclass
class Pending:
    """Tracks in-flight command futures and timing milestones."""

    ack_fut: asyncio.Future[None]
    done_fut: asyncio.Future[Message]
    t_first_send: float = 0.0
    t_ack: float = 0.0
    t_done: float = 0.0

