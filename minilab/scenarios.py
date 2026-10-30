"""Named workflow scenarios and system test fixtures.

Defines operational workflows (basic, contention, degraded, critical, estop,
deadlock) and scenario configurations for CLI execution and integration testing.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from minilab.host.workflow import Step, Workflow
from minilab.transport import FAULT_PRESETS, FaultConfig, LinkFaults


def basic_workflow() -> Workflow:
    """Standard happy-path workflow with parallel heating and priming."""
    return Workflow(
        name="basic",
        steps=[
            Step(id="heat", device="heater1", action="set_temp", params={"target": 40.0}),
            Step(id="prime", device="pump1", action="prime", params={"ml": 1.0}),
            Step(
                id="move_a",
                device="arm1",
                action="move_to",
                params={"x": 20.0, "y": 30.0},
                zones=["A"],
                depends_on=["heat", "prime"],
            ),
            Step(
                id="dispense",
                device="pump1",
                action="dispense",
                params={"ml": 5.0},
                zones=["A"],
                depends_on=["move_a"],
            ),
            Step(
                id="home",
                device="arm1",
                action="home",
                depends_on=["dispense"],
            ),
        ],
    )


def contention_workflow() -> Workflow:
    """Contention scenario where two independent arm steps contend for Zone A."""
    return Workflow(
        name="contention",
        steps=[
            Step(
                id="arm1_move",
                device="arm1",
                action="move_to",
                params={"x": 10.0, "y": 10.0},
                zones=["A"],
            ),
            Step(
                id="arm2_move",
                device="arm2",
                action="move_to",
                params={"x": 90.0, "y": 90.0},
                zones=["A"],
            ),
        ],
    )


def degraded_workflow() -> Workflow:
    """Degradation workflow: pump failure skips arm1, while heater and arm2 proceed."""
    return Workflow(
        name="degraded",
        steps=[
            Step(id="heat", device="heater1", action="set_temp", params={"target": 40.0}),
            Step(id="prime", device="pump1", action="prime", params={"ml": 1.0}),
            Step(
                id="move_a",
                device="arm1",
                action="move_to",
                params={"x": 20.0, "y": 30.0},
                zones=["A"],
                depends_on=["prime"],
            ),
            Step(
                id="dispense",
                device="pump1",
                action="dispense",
                params={"ml": 5.0},
                zones=["A"],
                depends_on=["move_a"],
            ),
            Step(
                id="other_arm",
                device="arm2",
                action="move_to",
                params={"x": 80.0, "y": 80.0},
                zones=["C"],
            ),
        ],
    )


def critical_workflow() -> Workflow:
    """Workflow containing a critical step that triggers e-stop on failure."""
    return Workflow(
        name="critical",
        steps=[
            Step(id="heat", device="heater1", action="set_temp", params={"target": 40.0}),
            Step(
                id="prime",
                device="pump1",
                action="prime",
                params={"ml": 1.0},
                critical=True,
            ),
            Step(
                id="move_a",
                device="arm1",
                action="move_to",
                params={"x": 20.0, "y": 30.0},
                zones=["A"],
                depends_on=["prime"],
            ),
            Step(
                id="dispense",
                device="pump1",
                action="dispense",
                params={"ml": 5.0},
                zones=["A"],
                depends_on=["move_a"],
            ),
            Step(
                id="other_arm",
                device="arm2",
                action="move_to",
                params={"x": 80.0, "y": 80.0},
                zones=["C"],
            ),
        ],
    )


def deadlock_workflow() -> Workflow:
    """Opposite-order zone acquisition scenario between two arms."""
    return Workflow(
        name="deadlock",
        steps=[
            Step(
                id="step1",
                device="arm1",
                action="move_to",
                params={"x": 10.0, "y": 10.0},
                zones=["A", "B"],
            ),
            Step(
                id="step2",
                device="arm2",
                action="move_to",
                params={"x": 90.0, "y": 90.0},
                zones=["B", "A"],
            ),
        ],
    )


def dispense_workflow() -> Workflow:
    """Single pump dispense step."""
    return Workflow(
        name="dispense",
        steps=[
            Step(id="dispense", device="pump1", action="dispense", params={"ml": 5.0}),
        ],
    )


def estop_workflow() -> Workflow:
    """Long-running parallel steps designed to be interrupted by mid-flight e-stop."""
    return Workflow(
        name="estop",
        steps=[
            Step(
                id="arm_move",
                device="arm1",
                action="move_to",
                params={"x": 100.0, "y": 100.0},
            ),
            Step(id="pump_dispense", device="pump1", action="dispense", params={"ml": 30.0}),
            Step(id="heat_step", device="heater1", action="set_temp", params={"target": 60.0}),
        ],
    )


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
            expected="SUCCESS",  # Or DEGRADED depending on seed
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
