"""Workflow execution engine — walks the DAG and executes steps.

The host supplies permission checking and real action execution. The built-in
action executor simulates success; custom executors can pause for approval.

Data flows through a shared context dict that steps can read/write.
"""

from __future__ import annotations

import logging
import operator
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional

from koda_workflows.models import (
    ConditionOperator,
    RunStatus,
    StepResult,
    StepType,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStep,
)
from koda_workflows.storage import WorkflowStorage

logger = logging.getLogger("koda_workflows.engine")

# Operator mapping for condition evaluation
_OPS = {
    ConditionOperator.EQUALS: operator.eq,
    ConditionOperator.NOT_EQUALS: operator.ne,
    ConditionOperator.GREATER_THAN: operator.gt,
    ConditionOperator.LESS_THAN: operator.lt,
}


class WorkflowEngine:
    """Executes workflow definitions step by step.

    For Phase 3 MVP, execution is synchronous. Phase 4 will add
    async execution with Celery/background workers.
    """

    def __init__(self, storage: WorkflowStorage) -> None:
        self._storage = storage

    def execute(
        self,
        workflow: WorkflowDefinition,
        initial_context: Optional[dict[str, Any]] = None,
        triggered_by: str = "manual",
    ) -> WorkflowRun:
        """Execute a workflow from its trigger step through the DAG.

        Returns the completed (or failed/paused) WorkflowRun.
        """
        # Validate first
        errors = workflow.validate_dag()
        if errors:
            run = WorkflowRun(
                workflow_id=workflow.id,
                workflow_version=workflow.version,
                status=RunStatus.FAILED,
                error=f"Validation errors: {'; '.join(errors)}",
                triggered_by=triggered_by,
                started_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
            )
            self._storage.save_run(run)
            return run

        # Create the run
        run = WorkflowRun(
            workflow_id=workflow.id,
            workflow_version=workflow.version,
            status=RunStatus.RUNNING,
            context=initial_context or {},
            definition_snapshot=workflow.model_copy(deep=True),
            triggered_by=triggered_by,
            started_at=datetime.now(timezone.utc),
        )
        self._storage.save_run(run)

        return self._continue(workflow, run)

    def resume(self, run_id: str) -> Optional[WorkflowRun]:
        """Continue after the host has approved and completed the paused action.

        The host owns authentication, approval and execution of that action.
        This method does not itself grant permission or repeat its side effects.
        """
        run = self._storage.get_run(run_id)
        if not run or run.status != RunStatus.WAITING_APPROVAL:
            return None
        workflow = run.definition_snapshot
        if workflow is None:
            workflow = self._storage.get_workflow(run.workflow_id)
            if workflow is None or workflow.version != run.workflow_version:
                run.status = RunStatus.FAILED
                run.error = "Legacy run definition is missing or changed; cannot safely resume"
                run.completed_at = datetime.now(timezone.utc)
                self._storage.save_run(run)
                return run
            run.definition_snapshot = workflow.model_copy(deep=True)
        current = next((r for r in reversed(run.step_results)
                        if r.step_id == run.current_step_id and r.status == "waiting_approval"), None)
        if current is None:
            run.status = RunStatus.FAILED
            run.error = "Missing paused step result"
            run.completed_at = datetime.now(timezone.utc)
            self._storage.save_run(run)
            return run
        current.status = "completed"
        current.completed_at = datetime.now(timezone.utc)
        self._merge_output(run, current)
        run.status = RunStatus.RUNNING
        return self._continue(workflow, run)

    @staticmethod
    def _successors(step: WorkflowStep, result: StepResult | None = None) -> list[str]:
        if step.type != StepType.CONDITION:
            return step.next_steps
        if result is None:
            return list(filter(None, [step.config.get("true_step"), step.config.get("false_step")]))
        output = result.output
        branch = output.get("branch", "false") if isinstance(output, dict) else ("true" if output else "false")
        target = step.config.get(f"{branch}_step")
        return [target] if target else []

    @staticmethod
    def _merge_output(run: WorkflowRun, result: StepResult) -> None:
        if isinstance(result.output, dict):
            run.context.update(result.output)
        elif result.output is not None:
            run.context[f"step_{result.step_id}_output"] = result.output

    def _continue(self, workflow: WorkflowDefinition, run: WorkflowRun) -> WorkflowRun:
        """Schedule nodes only when all possible incoming branches are resolved.

        The execution frontier is reconstructed from persisted results and the
        immutable definition. Inactive branches are skipped, including their
        descendants, without blocking a join reached by an active branch.
        """
        step_map = workflow.step_map
        trigger = workflow.trigger_step
        if trigger is None:
            run.status = RunStatus.FAILED
            run.error = "No trigger step found"
        else:
            reachable = set()
            pending = [trigger.id]
            while pending:
                sid = pending.pop()
                if sid in reachable:
                    continue
                reachable.add(sid)
                pending.extend(self._successors(step_map[sid]))
            predecessors = {sid: set() for sid in reachable}
            for sid in reachable:
                for target in self._successors(step_map[sid]):
                    predecessors[target].add(sid)
            completed = {r.step_id: r for r in run.step_results if r.status == "completed"}
            skipped = set()
            while reachable - completed.keys() - skipped:
                progress = False
                for step in workflow.steps:
                    sid = step.id
                    if sid not in reachable or sid in completed or sid in skipped:
                        continue
                    parents = predecessors[sid]
                    if not parents <= completed.keys() | skipped:
                        continue
                    active = sid == trigger.id or any(
                        parent in completed and sid in self._successors(step_map[parent], completed[parent])
                        for parent in parents
                    )
                    progress = True
                    if not active:
                        skipped.add(sid)
                        continue
                    run.current_step_id = sid
                    self._storage.save_run(run)
                    result = self._execute_step(step, run.context)
                    run.step_results.append(result)
                    if result.status == "waiting_approval":
                        run.status = RunStatus.WAITING_APPROVAL
                        self._storage.save_run(run)
                        return run
                    if result.status != "completed":
                        run.status = RunStatus.FAILED
                        run.error = f"Step '{sid}' failed: {result.error or result.status}"
                        break
                    completed[sid] = result
                    self._merge_output(run, result)
                    self._storage.save_run(run)
                if run.status == RunStatus.FAILED:
                    break
                if not progress:
                    run.status = RunStatus.FAILED
                    run.error = "Unresolved workflow dependencies"
                    break
        if run.status == RunStatus.RUNNING:
            run.status = RunStatus.COMPLETED
        run.completed_at = datetime.now(timezone.utc)
        run.current_step_id = None
        self._storage.save_run(run)
        return run

    # ------------------------------------------------------------------
    # Step executors
    # ------------------------------------------------------------------

    def _execute_step(
        self, step: WorkflowStep, context: dict[str, Any]
    ) -> StepResult:
        """Execute a single step and return the result."""
        start = time.monotonic()
        started_at = datetime.now(timezone.utc)

        try:
            if step.type == StepType.TRIGGER:
                output = self._exec_trigger(step, context)
            elif step.type == StepType.CONDITION:
                output = self._exec_condition(step, context)
            elif step.type == StepType.ACTION:
                output = self._exec_action(step, context)
            elif step.type == StepType.TRANSFORM:
                output = self._exec_transform(step, context)
            elif step.type == StepType.DELAY:
                output = self._exec_delay(step, context)
            else:
                return StepResult(
                    step_id=step.id,
                    status="failed",
                    error=f"Unknown step type: {step.type}",
                    started_at=started_at,
                )

            elapsed = (time.monotonic() - start) * 1000
            return StepResult(
                step_id=step.id,
                status="completed",
                output=output,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                duration_ms=round(elapsed, 2),
            )

        except Exception as e:
            elapsed = (time.monotonic() - start) * 1000
            logger.error("Step %s failed: %s", step.id, e)
            return StepResult(
                step_id=step.id,
                status="failed",
                error=str(e),
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                duration_ms=round(elapsed, 2),
            )

    def _exec_trigger(
        self, step: WorkflowStep, context: dict[str, Any]
    ) -> dict[str, Any]:
        """Trigger steps just pass through — they mark the workflow start."""
        trigger_type = step.config.get("trigger_type", "manual")
        return {"trigger_type": trigger_type, "triggered": True}

    def _exec_condition(
        self, step: WorkflowStep, context: dict[str, Any]
    ) -> dict[str, Any]:
        """Evaluate a condition and return which branch to take."""
        field = step.config.get("field", "")
        op_str = step.config.get("operator", "equals")
        value = step.config.get("value")

        # Resolve field from context (supports dot notation)
        actual = self._resolve_field(field, context)

        # Evaluate
        result = self._evaluate_condition(actual, op_str, value)
        return {"branch": "true" if result else "false", "evaluated": True}

    def _exec_action(
        self, step: WorkflowStep, context: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute an action step.

        For Phase 3 MVP, this is a simulated execution.
        Real execution will integrate with the agent's tool registry.
        """
        action_id = step.config.get("action_id", "unknown")
        service = step.config.get("service", "")
        parameters = step.config.get("parameters", {})

        # Interpolate context values into parameters
        resolved_params = self._interpolate(parameters, context)

        logger.info(
            "Executing action: %s (service: %s, params: %s)",
            action_id,
            service,
            resolved_params,
        )

        # MVP: Return a simulated success result
        # Phase 4: Call tool registry → permission check → execute → verify
        return {
            "action_id": action_id,
            "service": service,
            "parameters": resolved_params,
            "executed": True,
            "result": "simulated_success",
        }

    def _exec_transform(
        self, step: WorkflowStep, context: dict[str, Any]
    ) -> dict[str, Any]:
        """Transform data between steps."""
        operation = step.config.get("operation", "passthrough")
        source_field = step.config.get("source_field", "")
        target_field = step.config.get("target_field", "")
        template = step.config.get("template", "")

        if operation == "extract":
            value = self._resolve_field(source_field, context)
            return {target_field: value} if target_field else {"extracted": value}

        elif operation == "template":
            rendered = self._render_template(template, context)
            return {target_field: rendered} if target_field else {"rendered": rendered}

        elif operation == "merge":
            fields = step.config.get("fields", [])
            merged = {}
            for f in fields:
                val = self._resolve_field(f, context)
                if isinstance(val, dict):
                    merged.update(val)
                else:
                    merged[f] = val
            return {target_field: merged} if target_field else merged

        else:
            # passthrough
            return {}

    def _exec_delay(
        self, step: WorkflowStep, context: dict[str, Any]
    ) -> dict[str, Any]:
        """Delay step — wait for specified seconds.

        For MVP, we cap at 10 seconds. Real implementation will
        use background scheduling.
        """
        seconds = min(step.config.get("seconds", 1), 10)
        time.sleep(seconds)
        return {"delayed_seconds": seconds}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_field(field: str, context: dict[str, Any]) -> Any:
        """Resolve a dot-notation field path from context."""
        if not field:
            return None
        parts = field.split(".")
        current: Any = context
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current

    @staticmethod
    def _evaluate_condition(actual: Any, op_str: str, expected: Any) -> bool:
        """Evaluate a comparison condition."""
        try:
            op_enum = ConditionOperator(op_str)
        except ValueError:
            return False

        if op_enum == ConditionOperator.IS_EMPTY:
            return not actual
        if op_enum == ConditionOperator.IS_NOT_EMPTY:
            return bool(actual)
        if op_enum == ConditionOperator.CONTAINS:
            return expected in str(actual) if actual else False
        if op_enum == ConditionOperator.NOT_CONTAINS:
            return expected not in str(actual) if actual else True
        if op_enum == ConditionOperator.MATCHES_REGEX:
            return bool(re.search(str(expected), str(actual))) if actual else False

        fn = _OPS.get(op_enum)
        if fn:
            return fn(actual, expected)
        return False

    @staticmethod
    def _interpolate(params: dict, context: dict[str, Any]) -> dict:
        """Replace {{field}} placeholders in parameter values with context data."""
        result = {}
        for k, v in params.items():
            if isinstance(v, str):
                def replacer(match: re.Match) -> str:
                    field = match.group(1).strip()
                    val = WorkflowEngine._resolve_field(field, context)
                    return str(val) if val is not None else match.group(0)

                result[k] = re.sub(r"\{\{(.+?)\}\}", replacer, v)
            else:
                result[k] = v
        return result

    @staticmethod
    def _render_template(template: str, context: dict[str, Any]) -> str:
        """Render a simple {{field}} template."""
        def replacer(match: re.Match) -> str:
            field = match.group(1).strip()
            val = WorkflowEngine._resolve_field(field, context)
            return str(val) if val is not None else ""

        return re.sub(r"\{\{(.+?)\}\}", replacer, template)
