# koda-workflows

DAG-based workflow automation engine with branching, conditions, pause/resume, and SQLite persistence.

## What & why

`koda_workflows` is the workflow engine extracted from [Koda AI](https://github.com/HeyKodaAI) — "the AI agent that never lies" — part of the Koda project by Mike Wandzilak / Wandzilak Web Design. In Koda, it powers user-built automations: a workflow is a directed acyclic graph of steps that the engine validates, executes step by step, and records run by run.

The package is a self-contained Pydantic + SQLite core with an optional FastAPI router. Definitions are plain JSON-serializable models (each step even carries an `x`/`y` canvas position), so it works equally well behind a visual editor or driven entirely from code.

## Features

- **Five step types**: `TRIGGER` (manual, schedule, webhook, event), `CONDITION` (if/else branch), `ACTION`, `TRANSFORM` (extract / template / merge), `DELAY`.
- **DAG validation before every run** — `validate_dag()` checks for a trigger step, dangling `next_steps` references, missing/invalid condition branch targets, duplicate step ids, and cycles (DFS). A definition that fails validation produces a `FAILED` run with the errors, never a partial execution.
- **Branching conditions** with nine operators (`equals`, `not_equals`, `contains`, `not_contains`, `greater_than`, `less_than`, `is_empty`, `is_not_empty`, `matches_regex`), evaluated against a dot-notation field path into the run context (`lead.budget`).
- **Shared run context** — each step's output merges into a context dict that downstream steps read; string parameters support `{{field.path}}` interpolation.
- **Pause/resume** — a step that reports `waiting_approval` parks the run in `WAITING_APPROVAL`; `engine.resume(run_id)` picks up from the current step's successors, skipping already-executed steps.
- **SQLite persistence** (WAL mode) for workflow definitions (versioned, JSON-serialized) and full run history with per-step results, timings, and errors.
- **FastAPI router** exposing CRUD, validate, execute, duplicate, and run management under `/api/v1/workflows/*`.

Scope note: in this package, `ACTION` steps resolve their parameters and return a structured `simulated_success` result rather than calling external services — the intended integration point is wiring `WorkflowEngine._exec_action` to your own tool registry / permission layer (in Koda, its companion is [koda-permissions](https://github.com/HeyKodaAI/koda-permissions)). `DELAY` steps sleep synchronously, capped at 10 seconds.

## Install

Not yet on PyPI. Requires Python 3.11+.

```bash
pip install git+https://github.com/HeyKodaAI/koda-workflows.git
```

Dependencies: `pydantic>=2.0`, `fastapi>=0.110`.

## Quickstart

```python
from koda_workflows.engine import WorkflowEngine
from koda_workflows.models import StepType, WorkflowDefinition, WorkflowStep
from koda_workflows.storage import WorkflowStorage

# ":memory:" for the demo; pass a file path (e.g. "data/workflows.db") to persist
storage = WorkflowStorage(":memory:")
engine = WorkflowEngine(storage)

workflow = WorkflowDefinition(
    name="Triage inbound lead",
    steps=[
        WorkflowStep(
            id="start", type=StepType.TRIGGER,
            config={"trigger_type": "manual"},
            next_steps=["is_hot"],
        ),
        WorkflowStep(
            id="is_hot", type=StepType.CONDITION,
            config={
                "field": "lead.budget", "operator": "greater_than", "value": 5000,
                "true_step": "notify", "false_step": "file_away",
            },
        ),
        WorkflowStep(
            id="notify", type=StepType.ACTION,
            config={
                "action_id": "slack:post_message", "service": "slack",
                "parameters": {"text": "Hot lead: {{lead.name}} (budget {{lead.budget}})"},
            },
        ),
        WorkflowStep(
            id="file_away", type=StepType.ACTION,
            config={
                "action_id": "filesystem.write_files", "service": "filesystem",
                "parameters": {"note": "Filed lead {{lead.name}}"},
            },
        ),
    ],
)

print("validation errors:", workflow.validate_dag())  # []
storage.save_workflow(workflow)

run = engine.execute(workflow, initial_context={"lead": {"name": "Paula", "budget": 12000}})
print("run status:", run.status.value)
for sr in run.step_results:
    print(f"  {sr.step_id}: {sr.status} -> {sr.output}")
```

Output:

```
validation errors: []
run status: completed
  start: completed -> {'trigger_type': 'manual', 'triggered': True}
  is_hot: completed -> {'branch': 'true', 'evaluated': True}
  notify: completed -> {'action_id': 'slack:post_message', 'service': 'slack', 'parameters': {'text': 'Hot lead: Paula (budget 12000)'}, 'executed': True, 'result': 'simulated_success'}
```

Only the taken branch executes — `file_away` never runs.

## API overview

- **`koda_workflows.engine.WorkflowEngine`** — `execute(workflow, initial_context=None, triggered_by="manual") -> WorkflowRun` walks the DAG from the trigger; `resume(run_id)` continues a `WAITING_APPROVAL` run. Per-step results include output, error, and `duration_ms`.
- **`koda_workflows.models`** — `WorkflowDefinition` (with `validate_dag()`, `trigger_step`, `step_map`), `WorkflowStep`, `WorkflowRun`, `StepResult`, plus the enums `StepType`, `TriggerType`, `ConditionOperator`, `WorkflowStatus` (draft/active/paused/archived), `RunStatus` (pending/running/completed/failed/cancelled/waiting_approval).
- **`koda_workflows.storage.WorkflowStorage`** — SQLite backend: `save_workflow`, `get_workflow`, `list_workflows`, `delete_workflow` (cascades to runs), `count_workflows`; `save_run`, `get_run`, `list_runs`, `count_runs`.
- **`koda_workflows.routes`** — FastAPI `router` (prefix `/api/v1/workflows`): list/create/get/update/delete, `POST /{id}/execute`, `POST /{id}/validate`, `POST /{id}/duplicate`, plus `GET /runs`, `GET /runs/{run_id}`, `POST /runs/{run_id}/resume`, `POST /runs/{run_id}/cancel`. Call `init_workflow_routes(storage)` at startup, then `app.include_router(router)`.

## Testing

```bash
pip install -e . pytest pytest-asyncio
pytest
```

31 tests covering models/validation, the engine (branching, transforms, interpolation, failure paths), and storage.

## License

MIT
