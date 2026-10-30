"""Workflow DAG models, step definitions, and graph cycle validation.

Defines the dependency graph of device execution steps, validation rules,
and execution status tracking.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from minilab.errors import WorkflowValidationError


class StepStatus(str, Enum):
    """Execution status lifecycle states for a workflow step."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


@dataclass
class Step:
    """A discrete operation executed on a device within a workflow."""

    id: str
    device: str
    action: str
    params: dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    zones: list[str] = field(default_factory=list)
    critical: bool = False
    ack_timeout: float | None = None
    done_timeout: float | None = None


@dataclass
class StepResult:
    """Execution result and timing telemetry for a workflow step."""

    step: Step
    status: StepStatus = StepStatus.PENDING
    started_at: float | None = None
    finished_at: float | None = None
    attempts: int = 0
    output: dict[str, Any] = field(default_factory=dict)
    error: str = ""


@dataclass
class WorkflowResult:
    """Outcome summary for an entire workflow run."""

    name: str
    status: str  # "SUCCESS" | "DEGRADED" | "ABORTED"
    steps: dict[str, StepResult]
    duration: float


@dataclass
class Workflow:
    """A directed acyclic graph (DAG) of execution steps."""

    name: str
    steps: list[Step]

    def validate(self, devices: set[str], zones: set[str]) -> None:
        """Validate step ids, device/zone targets, and DAG acyclicity using Kahn's algorithm."""
        step_ids = set()
        for step in self.steps:
            if step.id in step_ids:
                raise WorkflowValidationError(f"Duplicate step id detected: {step.id!r}")
            step_ids.add(step.id)

            if step.device not in devices:
                raise WorkflowValidationError(
                    f"Step {step.id!r} targets unknown device: {step.device!r}"
                )

            # Check duplicate zones within a step
            if len(step.zones) != len(set(step.zones)):
                raise WorkflowValidationError(
                    f"Step {step.id!r} contains duplicate zones: {step.zones}"
                )

            for z in step.zones:
                if z not in zones:
                    raise WorkflowValidationError(
                        f"Step {step.id!r} references unknown zone: {z!r}"
                    )

        # Check unknown dependencies
        for step in self.steps:
            for dep in step.depends_on:
                if dep not in step_ids:
                    raise WorkflowValidationError(
                        f"Step {step.id!r} has unknown dependency: {dep!r}"
                    )

        # Cycle detection using Kahn's algorithm
        in_degree: dict[str, int] = {s.id: 0 for s in self.steps}
        dependents: dict[str, list[str]] = defaultdict(list)

        for step in self.steps:
            for dep in step.depends_on:
                in_degree[step.id] += 1
                dependents[dep].append(step.id)

        queue = deque([sid for sid, deg in in_degree.items() if deg == 0])
        visited_count = 0

        while queue:
            node = queue.popleft()
            visited_count += 1
            for child in dependents[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if visited_count < len(self.steps):
            cycle_nodes = sorted([sid for sid, deg in in_degree.items() if deg > 0])
            raise WorkflowValidationError(
                f"Dependency cycle detected in workflow involving steps: {cycle_nodes}"
            )
