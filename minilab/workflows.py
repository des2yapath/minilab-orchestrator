"""Pre-defined standard workflow DAG factories.

Provides basic, contention, degraded, critical, deadlock, dispense, and estop
workflow templates.
"""

from __future__ import annotations

from minilab.host.workflow import Step, Workflow


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

