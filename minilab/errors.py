"""Exception hierarchy for minilab.

Defines all custom exceptions used across device simulators, transport layers,
and host orchestration.
"""

from __future__ import annotations


class MiniLabError(Exception):
    """Base exception for all minilab controller errors."""


class FrameError(MiniLabError):
    """Raised when a protocol frame fails validation (CRC, JSON, structure)."""


class DeviceError(MiniLabError):
    """Raised when a device execution fails.

    The string message is a standard error code (e.g. STALL, OCCLUSION, SENSOR_FAULT).
    """


class CommandTimeout(MiniLabError):
    """Raised when a command fails to receive an ACK or DONE within the timeout."""


class DeviceOffline(MiniLabError):
    """Raised for fast failure when sending commands to a previously timed-out device."""


class EStopped(MiniLabError):
    """Raised when an operation is attempted while the emergency stop latch is active."""


class ZoneTimeout(MiniLabError):
    """Raised when acquisition of a physical zone lock times out."""


class WorkflowValidationError(MiniLabError):
    """Raised when a workflow DAG is invalid (cycles, missing dependencies, bad zones)."""
