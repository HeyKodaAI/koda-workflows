"""Workflow data models — definitions, steps, triggers, runs.

A workflow is a directed acyclic graph (DAG) of steps. Each step has:
- A type (trigger, condition, action, transform, delay)
- A configuration dict specific to the step type
- Connections to downstream steps (edges)

Execution proceeds from trigger → through conditions → to actions,
with the host responsible for action permissions and real execution.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class StepType(str, Enum):
    """Types of workflow steps."""

    TRIGGER = "trigger"        # What starts the workflow (manual, schedule, webhook, event)
    CONDITION = "condition"    # Branch based on data (if/else)
    ACTION = "action"          # Perform something (send email, create file, call API)
    TRANSFORM = "transform"    # Reshape data between steps (extract, format, merge)
    DELAY = "delay"            # Wait before continuing (seconds, until time)


class TriggerType(str, Enum):
    """Trigger subtypes — what starts a workflow."""

    MANUAL = "manual"          # User clicks "Run"
    SCHEDULE = "schedule"      # Cron-like schedule
    WEBHOOK = "webhook"        # External HTTP POST
    EVENT = "event"            # Internal event (email received, file changed, etc.)


class ConditionOperator(str, Enum):
    """Comparison operators for condition steps."""

    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    IS_EMPTY = "is_empty"
    IS_NOT_EMPTY = "is_not_empty"
    MATCHES_REGEX = "matches_regex"


class WorkflowStep(BaseModel):
    """A single step in a workflow DAG."""

    id: str = Field(..., description="Unique step ID within the workflow (e.g. 'step_1')")
    type: StepType
    name: str = Field(default="", description="Human-readable step name")
    description: str = Field(default="")

    # Step-specific configuration
    config: dict[str, Any] = Field(
        default_factory=dict,
        description="Type-specific config. "
        "Trigger: {trigger_type, schedule, webhook_path, event_type}. "
        "Condition: {field, operator, value, true_step, false_step}. "
        "Action: {action_id, service, parameters}. "
        "Transform: {operation, source_field, target_field, template}. "
        "Delay: {seconds, until}.",
    )

    # DAG edges — IDs of next steps
    next_steps: list[str] = Field(
        default_factory=list,
        description="Step IDs to execute after this one (for non-branching steps)",
    )

    # Position for visual editor (x, y coordinates)
    position: dict[str, float] = Field(
        default_factory=lambda: {"x": 0, "y": 0},
        description="Canvas position for the visual editor",
    )


class WorkflowStatus(str, Enum):
    """Lifecycle status of a workflow definition."""

    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


class WorkflowDefinition(BaseModel):
    """Complete workflow definition — the thing users create and edit."""

    id: str = Field(default="", description="Workflow UUID")
    name: str = Field(..., min_length=1, max_length=128)
    description: str = Field(default="")
    status: WorkflowStatus = WorkflowStatus.DRAFT
    version: int = Field(default=1, ge=1)

    # The DAG
    steps: list[WorkflowStep] = Field(default_factory=list)

    # Metadata
    tags: list[str] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def trigger_step(self) -> Optional[WorkflowStep]:
        """Get the entry-point trigger step (first trigger found)."""
        for step in self.steps:
            if step.type == StepType.TRIGGER:
                return step
        return None

    @property
    def step_map(self) -> dict[str, WorkflowStep]:
        """Map of step ID → step for quick lookup."""
        return {s.id: s for s in self.steps}

    def validate_dag(self) -> list[str]:
        """Validate the workflow DAG structure. Returns list of errors."""
        errors: list[str] = []
        step_ids = {s.id for s in self.steps}

        # Must have at least one trigger
        triggers = [s for s in self.steps if s.type == StepType.TRIGGER]
        if not triggers:
            errors.append("Workflow must have at least one trigger step")

        # All next_steps must reference valid step IDs
        for step in self.steps:
            for ns in step.next_steps:
                if ns not in step_ids:
                    errors.append(
                        f"Step '{step.id}' references unknown next step '{ns}'"
                    )

        # Condition steps must define true_step/false_step in config
        for step in self.steps:
            if step.type == StepType.CONDITION:
                if "true_step" not in step.config:
                    errors.append(
                        f"Condition step '{step.id}' missing 'true_step' in config"
                    )
                if "false_step" not in step.config:
                    errors.append(
                        f"Condition step '{step.id}' missing 'false_step' in config"
                    )
                # Validate branch targets exist
                for branch in ("true_step", "false_step"):
                    target = step.config.get(branch)
                    if target and target not in step_ids:
                        errors.append(
                            f"Condition step '{step.id}' {branch} references "
                            f"unknown step '{target}'"
                        )

        # No duplicate IDs
        if len(step_ids) != len(self.steps):
            errors.append("Duplicate step IDs found")

        # Cycle detection (simple DFS)
        visited: set[str] = set()
        in_stack: set[str] = set()

        def has_cycle(sid: str) -> bool:
            if sid in in_stack:
                return True
            if sid in visited:
                return False
            visited.add(sid)
            in_stack.add(sid)
            step = self.step_map.get(sid)
            if step:
                neighbors = list(step.next_steps)
                if step.type == StepType.CONDITION:
                    neighbors.extend(
                        filter(None, [
                            step.config.get("true_step"),
                            step.config.get("false_step"),
                        ])
                    )
                for n in neighbors:
                    if has_cycle(n):
                        return True
            in_stack.discard(sid)
            return False

        for s in self.steps:
            if has_cycle(s.id):
                errors.append("Workflow contains a cycle")
                break

        return errors


class RunStatus(str, Enum):
    """Status of a workflow execution run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    WAITING_APPROVAL = "waiting_approval"


class StepResult(BaseModel):
    """Result of executing a single step."""

    step_id: str
    status: str = "completed"
    output: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None


class WorkflowRun(BaseModel):
    """Record of a single workflow execution."""

    id: str = Field(default="", description="Run UUID")
    workflow_id: str
    workflow_version: int = 1
    definition_snapshot: Optional[WorkflowDefinition] = None
    status: RunStatus = RunStatus.PENDING

    # Step-by-step results
    step_results: list[StepResult] = Field(default_factory=list)
    current_step_id: Optional[str] = None

    # Input/output data flowing through the workflow
    context: dict[str, Any] = Field(
        default_factory=dict,
        description="Shared data context that steps read from and write to",
    )

    # Execution metadata
    triggered_by: str = Field(default="manual", description="What triggered this run")
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
