"""Concurrent DAG workflow execution engine.

Spawns an asyncio task per workflow step, coordinating inter-step dependencies via
asyncio.Events without manual topological sorting, acquiring required zones, and
propagating graceful degradation or emergency stop triggers.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from minilab.errors import CommandTimeout, DeviceError, DeviceOffline, EStopped, ZoneTimeout
from minilab.host.workflow import Step, StepResult, StepStatus, Workflow, WorkflowResult

if TYPE_CHECKING:
    from minilab.host.orchestrator import Orchestrator


class WorkflowRunner:
    """Executes a workflow DAG concurrently."""

    def __init__(self, orch: Orchestrator, wf: Workflow) -> None:
        self.orch = orch
        self.wf = wf
        self.log = orch.log

    async def run(self) -> WorkflowResult:
        """Execute all steps according to dependency ordering."""
        t0 = time.perf_counter()
        results = {s.id: StepResult(s) for s in self.wf.steps}
        events = {s.id: asyncio.Event() for s in self.wf.steps}
        tasks = [
            asyncio.create_task(self._run_step(s, results, events, t0), name=f"step:{s.id}")
            for s in self.wf.steps
        ]
        self.orch.register_step_tasks(tasks)
        self.log.emit(
            "runner",
            "workflow_start",
            name=self.wf.name,
            steps=[s.id for s in self.wf.steps],
        )

        unexpected_error: Exception | None = None
        try:
            task_outcomes = await asyncio.gather(*tasks, return_exceptions=True)
            for out in task_outcomes:
                if (
                    isinstance(out, Exception)
                    and not isinstance(out, asyncio.CancelledError)
                    and unexpected_error is None
                ):
                    unexpected_error = out
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            self.orch.unregister_step_tasks(tasks)

        if unexpected_error is not None:
            self.log.emit("runner", "unexpected_bug", error=str(unexpected_error))
            raise unexpected_error

        all_done = all(r.status == StepStatus.DONE for r in results.values())
        status = (
            "ABORTED"
            if self.orch.estop_state.active
            else ("SUCCESS" if all_done else "DEGRADED")
        )
        duration = time.perf_counter() - t0
        self.log.emit(
            "runner",
            "workflow_end",
            name=self.wf.name,
            status=status,
            duration=round(duration, 6),
        )
        return WorkflowResult(self.wf.name, status, results, duration)

    async def _run_step(
        self,
        step: Step,
        results: dict[str, StepResult],
        events: dict[str, asyncio.Event],
        t0: float,
    ) -> None:
        res = results[step.id]
        try:
            for dep in step.depends_on:
                await events[dep].wait()

            if any(results[d].status is not StepStatus.DONE for d in step.depends_on):
                res.status = StepStatus.SKIPPED
                res.error = "dependency did not complete"
                self.log.emit(
                    "runner",
                    "step_skipped",
                    run=self.wf.name,
                    step=step.id,
                    device=step.device,
                    action=step.action,
                    reason=res.error,
                )
                return

            res.status = StepStatus.RUNNING
            owner = f"{self.wf.name}:{step.id}"
            async with self.orch.zones.hold(owner, step.zones, timeout=self.orch.zone_timeout):
                res.started_at = time.perf_counter() - t0
                self.log.emit(
                    "runner",
                    "step_start",
                    run=self.wf.name,
                    step=step.id,
                    device=step.device,
                    action=step.action,
                )
                cr = await self.orch.clients[step.device].call(
                    step.action,
                    step.params,
                    ack_timeout=step.ack_timeout,
                    done_timeout=step.done_timeout,
                )
                res.finished_at = time.perf_counter() - t0
                res.output = cr.output
                res.attempts = cr.attempts

            res.status = StepStatus.DONE
            self.log.emit(
                "runner",
                "step_done",
                run=self.wf.name,
                step=step.id,
                device=step.device,
                action=step.action,
            )
        except asyncio.CancelledError:
            res.status = StepStatus.CANCELLED
            self.log.emit(
                "runner",
                "step_cancelled",
                run=self.wf.name,
                step=step.id,
                device=step.device,
                action=step.action,
            )
            raise
        except (CommandTimeout, DeviceError, DeviceOffline, EStopped, ZoneTimeout) as e:
            res.status = StepStatus.FAILED
            res.error = f"{type(e).__name__}: {e}"
            self.log.emit(
                "runner",
                "step_failed",
                run=self.wf.name,
                step=step.id,
                device=step.device,
                action=step.action,
                error=res.error,
            )
            if step.critical and not self.orch.estop_state.active:
                await self.orch.estop(f"critical step {step.id} failed")
        finally:
            if res.finished_at is None and res.started_at is not None:
                res.finished_at = time.perf_counter() - t0
            events[step.id].set()
