"""Named workflow scenarios and system test fixtures.

Defines operational scenario configurations mapping scenario names to workflow
factories and execution overrides for CLI invocation and system testing.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from minilab.host.workflow import Workflow
from minilab.transport import FAULT_PRESETS, FaultConfig, LinkFaults
from minilab.workflows import (
    basic_workflow,
    contention_workflow,
    critical_workflow,
    deadlock_workflow,
    degraded_workflow,
    dispense_workflow,
    estop_workflow,
)

__all__ = [
    "Scenario",
    "basic_workflow",
    "contention_workflow",
    "critical_workflow",
    "deadlock_workflow",
    "degraded_workflow",
    "dispense_workflow",
    "estop_workflow",
    "get_scenarios",
]


@dataclass
class Scenario:
    """Preconfigured scenario specification."""

    name: str
    description: str
    workflow_factory: Callable[[], Workflow]
    config_overrides: dict[str, Any] = field(default_factory=dict)
    expected: str = "SUCCESS"
    estop_after: float | None = None
    settle: float | None = None


def get_scenarios() -> dict[str, Scenario]:
    """Return dictionary of all standard test and demo scenarios."""
    return {
        "basic": Scenario(
            name="basic",
            description="Happy path: parallel heating and priming, sequential pipetting",
            workflow_factory=basic_workflow,
            expected="SUCCESS",
        ),
        "contention": Scenario(
            name="contention",
            description="Two arms competing for Zone A: serialised execution",
            workflow_factory=contention_workflow,
            expected="SUCCESS",
        ),
        "degraded": Scenario(
            name="degraded",
            description="Pump1 fails; dependents skipped, heater and arm2 complete",
            workflow_factory=degraded_workflow,
            config_overrides={"device_opts": {"pump1": {"always_fail": True}}},
            expected="DEGRADED",
        ),
        "critical": Scenario(
            name="critical",
            description="Critical step fails, triggering e-stop and aborting workflow",
            workflow_factory=critical_workflow,
            config_overrides={"device_opts": {"pump1": {"always_fail": True}}},
            expected="ABORTED",
        ),
        "estop": Scenario(
            name="estop",
            description="Manual emergency stop triggered mid-operation during execution",
            workflow_factory=estop_workflow,
            config_overrides={"time_scale": 1.0},
            estop_after=0.3,
            expected="ABORTED",
        ),
        "lossy": Scenario(
            name="lossy",
            description="Heavy fault injection preset across all links",
            workflow_factory=basic_workflow,
            config_overrides={
                "faults": {
                    d: FAULT_PRESETS["heavy"] for d in ("arm1", "arm2", "pump1", "heater1")
                }
            },
            expected="SUCCESS",
        ),
        "disconnect": Scenario(
            name="disconnect",
            description="Cable disconnected mid-workflow causing command timeout",
            workflow_factory=basic_workflow,
            config_overrides={
                "faults": {
                    "pump1": LinkFaults(
                        to_host=FaultConfig(disconnect_after=2)
                    )
                }
            },
            expected="DEGRADED",
        ),
        "deadlock-naive": Scenario(
            name="deadlock-naive",
            description="Unordered zone acquisition resulting in lock deadlock",
            workflow_factory=deadlock_workflow,
            config_overrides={"zone_ordered": False, "zone_step_delay": 0.01},
            expected="DEADLOCK",
        ),
        "deadlock-fixed": Scenario(
            name="deadlock-fixed",
            description="Global zone ordering preventing circular wait condition",
            workflow_factory=deadlock_workflow,
            config_overrides={"zone_ordered": True},
            expected="SUCCESS",
        ),
        "deadlock-timeout": Scenario(
            name="deadlock-timeout",
            description="Zone lock acquisition watchdog timeout breaking deadlock",
            workflow_factory=deadlock_workflow,
            config_overrides={
                "zone_ordered": False,
                "zone_step_delay": 0.01,
                "zone_timeout": 0.5,
            },
            expected="DEGRADED",
        ),
        "double-dispense": Scenario(
            name="double-dispense",
            description="Planted Bug #2: Retry on dropped ACK causes duplicate volume dispense",
            workflow_factory=dispense_workflow,
            config_overrides={
                "device_opts": {"pump1": {"dedup": False}},
                "faults": {"pump1": LinkFaults(to_host=FaultConfig(drop_first_n=1))},
                "ack_timeout": 0.25,
                "time_scale": 1.0,
            },
            settle=1.0,
            expected="SUCCESS",
        ),
        "fixed-dispense": Scenario(
            name="fixed-dispense",
            description="Fixed Bug #2: Idempotent deduplication discards redundant retry",
            workflow_factory=dispense_workflow,
            config_overrides={
                "faults": {"pump1": LinkFaults(to_host=FaultConfig(drop_first_n=1))},
                "ack_timeout": 0.25,
                "time_scale": 1.0,
            },
            settle=1.0,
            expected="SUCCESS",
        ),
    }
