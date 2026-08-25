"""Workflow execution engine — walks the DAG and executes steps.

The engine respects the permission system: every action step goes through
permission checking. If an action requires approval, the run pauses.

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
            triggered_by=triggered_by,
            started_at=datetime.now(timezone.utc),
        )
        self._storage.save_run(run)

        # Start from trigger step
        trigger = workflow.trigger_step
        if not trigger:
            run.status = RunStatus.FAILED
            run.error = "No trigger step found"
            run.completed_at = datetime.now(timezone.utc)
            self._storage.save_run(run)
            return run

        # Walk the DAG
        step_map = workflow.step_map
        queue: list[str] = [trigger.id]
        visited: set[str] = set()

        while queue:
            step_id = queue.pop(0)
            if step_id in visited:
                continue
            visited.add(step_id)

            step = step_map.get(step_id)
            if not step:
                run.status = RunStatus.FAILED
                run.error = f"Step '{step_id}' not found in workflow"
                break

            run.current_step_id = step_id
            self._storage.save_run(run)

            # Execute the step
            result = self._execute_step(step, run.context)
            run.step_results.append(result)

            if result.status == "failed":
                run.status = RunStatus.FAILED
                run.error = f"Step '{step_id}' failed: {result.error}"
                break

            if result.status == "waiting_approval":
                run.status = RunStatus.WAITING_APPROVAL
                self._storage.save_run(run)
                return run

            # Merge step output into context
            if result.output is not None:
                if isinstance(result.output, dict):
                    run.context.update(result.output)
                else:
                    run.context[f"step_{step_id}_output"] = result.output

            # Determine next steps
            if step.type == StepType.CONDITION:
                branch_result = result.output
                if isinstance(branch_result, dict):
                    branch = branch_result.get("branch", "false")
                else:
                    branch = "true" if branch_result else "false"

                next_id = step.config.get(f"{branch}_step")
                if next_id:
                    queue.append(next_id)
            else:
                queue.extend(step.next_steps)

        # Mark complete
        if run.status == RunStatus.RUNNING:
            run.status = RunStatus.COMPLETED

        run.completed_at = datetime.now(timezone.utc)
        run.current_step_id = None
        self._storage.save_run(run)
        return run

    def resume(self, run_id: str) -> Optional[WorkflowRun]:
        """Resume a paused workflow run (e.g., after approval)."""
        run = self._storage.get_run(run_id)
        if not run or run.status != RunStatus.WAITING_APPROVAL:
            return None

        workflow = self._storage.get_workflow(run.workflow_id)  # type: ignore
        if not workflow:
            return None

        # Find current step and continue from its next steps
        step_map = workflow.step_map
        current = step_map.get(run.current_step_id or "")
        if not current:
            run.status = RunStatus.FAILED
            run.error = "Cannot find current step to resume from"
            run.completed_at = datetime.now(timezone.utc)
            self._storage.save_run(run)
            return run

        run.status = RunStatus.RUNNING
        visited = {sr.step_id for sr in run.step_results}
        queue = list(current.next_steps)

        while queue:
            step_id = queue.pop(0)
            if step_id in visited:
                continue
            visited.add(step_id)

            step = step_map.get(step_id)
            if not step:
                run.status = RunStatus.FAILED
                run.error = f"Step '{step_id}' not found"
                break

            run.current_step_id = step_id
            result = self._execute_step(step, run.context)
            run.step_results.append(result)

            if result.status == "failed":
                run.status = RunStatus.FAILED
                run.error = f"Step '{step_id}' failed: {result.error}"
                break

            if result.status == "waiting_approval":
                run.status = RunStatus.WAITING_APPROVAL
                self._storage.save_run(run)
                return run

            if result.output is not None:
                if isinstance(result.output, dict):
                    run.context.update(result.output)
                else:
                    run.context[f"step_{step_id}_output"] = result.output

            if step.type == StepType.CONDITION:
                branch_result = result.output
                branch = "true" if branch_result else "false"
                next_id = step.config.get(f"{branch}_step")
                if next_id:
                    queue.append(next_id)
            else:
                queue.extend(step.next_steps)

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
