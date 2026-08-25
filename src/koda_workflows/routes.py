"""FastAPI routes for the workflow builder.

Endpoints:
    GET    /api/v1/workflows                    List workflows
    POST   /api/v1/workflows                    Create a workflow
    GET    /api/v1/workflows/{id}               Get workflow detail
    PUT    /api/v1/workflows/{id}               Update a workflow
    DELETE /api/v1/workflows/{id}               Delete a workflow
    POST   /api/v1/workflows/{id}/execute       Execute a workflow
    POST   /api/v1/workflows/{id}/validate      Validate workflow DAG
    POST   /api/v1/workflows/{id}/duplicate      Duplicate a workflow

    GET    /api/v1/workflows/runs               List all runs
    GET    /api/v1/workflows/runs/{run_id}       Get run detail
    POST   /api/v1/workflows/runs/{run_id}/resume  Resume a paused run
    POST   /api/v1/workflows/runs/{run_id}/cancel  Cancel a running/paused run
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from koda_workflows.engine import WorkflowEngine
from koda_workflows.models import (
    RunStatus,
    WorkflowDefinition,
    WorkflowStatus,
    WorkflowStep,
)
from koda_workflows.storage import WorkflowStorage

logger = logging.getLogger("koda_workflows.routes")

router = APIRouter(prefix="/api/v1/workflows", tags=["workflows"])

_storage: WorkflowStorage | None = None
_engine: WorkflowEngine | None = None


def init_workflow_routes(storage: WorkflowStorage) -> None:
    """Initialize workflow routes with storage backend."""
    global _storage, _engine
    _storage = storage
    _engine = WorkflowEngine(storage)


def _get_storage() -> WorkflowStorage:
    if _storage is None:
        raise RuntimeError("Workflow storage not initialized")
    return _storage


def _get_engine() -> WorkflowEngine:
    if _engine is None:
        raise RuntimeError("Workflow engine not initialized")
    return _engine


# ------------------------------------------------------------------
# Request models
# ------------------------------------------------------------------


class WorkflowCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    description: str = Field(default="")
    steps: list[dict[str, Any]] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)


class WorkflowUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, max_length=128)
    description: Optional[str] = None
    status: Optional[str] = None
    steps: Optional[list[dict[str, Any]]] = None
    tags: Optional[list[str]] = None


class ExecuteRequest(BaseModel):
    context: dict[str, Any] = Field(default_factory=dict)
    triggered_by: str = Field(default="manual")


# ------------------------------------------------------------------
# Runs (must come BEFORE workflow CRUD so /runs matches before /{workflow_id})
# ------------------------------------------------------------------


@router.get("/runs")
async def list_runs(
    workflow_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """List workflow runs."""
    storage = _get_storage()
    runs = storage.list_runs(
        workflow_id=workflow_id, status=status, limit=limit, offset=offset
    )
    total = storage.count_runs(workflow_id=workflow_id, status=status)
    return {
        "runs": [r.model_dump(mode="json") for r in runs],
        "count": len(runs),
        "total": total,
    }


@router.get("/runs/{run_id}")
async def get_run(run_id: str) -> dict:
    """Get detailed run information including step results."""
    storage = _get_storage()
    run = storage.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"run": run.model_dump(mode="json")}


@router.post("/runs/{run_id}/resume")
async def resume_run(run_id: str) -> dict:
    """Resume a paused workflow run."""
    engine = _get_engine()
    run = engine.resume(run_id)
    if not run:
        raise HTTPException(
            status_code=404,
            detail="Run not found or not in a resumable state",
        )
    return {
        "run_id": run.id,
        "status": run.status.value,
        "steps_executed": len(run.step_results),
    }


@router.post("/runs/{run_id}/cancel")
async def cancel_run(run_id: str) -> dict:
    """Cancel a running or paused workflow run."""
    storage = _get_storage()
    run = storage.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status not in (RunStatus.RUNNING, RunStatus.WAITING_APPROVAL, RunStatus.PENDING):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel run in '{run.status.value}' status",
        )
    run.status = RunStatus.CANCELLED
    from datetime import datetime, timezone
    run.completed_at = datetime.now(timezone.utc)
    storage.save_run(run)
    return {"run_id": run.id, "status": "cancelled", "message": "Run cancelled"}


# ------------------------------------------------------------------
# Workflow CRUD
# ------------------------------------------------------------------


@router.get("")
async def list_workflows(
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """List all workflows."""
    storage = _get_storage()
    workflows = storage.list_workflows(status=status, limit=limit, offset=offset)
    total = storage.count_workflows(status=status)
    return {
        "workflows": [_workflow_summary(w) for w in workflows],
        "count": len(workflows),
        "total": total,
    }


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_workflow(request: WorkflowCreateRequest) -> dict:
    """Create a new workflow."""
    storage = _get_storage()

    steps = [WorkflowStep(**s) for s in request.steps]
    workflow = WorkflowDefinition(
        name=request.name,
        description=request.description,
        steps=steps,
        tags=request.tags,
    )

    workflow_id = storage.save_workflow(workflow)
    logger.info("Created workflow: %s (%s)", request.name, workflow_id)
    return {"id": workflow_id, "message": "Workflow created"}


@router.get("/{workflow_id}")
async def get_workflow(workflow_id: str) -> dict:
    """Get full workflow detail."""
    storage = _get_storage()
    workflow = storage.get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return {"workflow": workflow.model_dump(mode="json")}


@router.put("/{workflow_id}")
async def update_workflow(workflow_id: str, request: WorkflowUpdateRequest) -> dict:
    """Update a workflow."""
    storage = _get_storage()
    existing = storage.get_workflow(workflow_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Workflow not found")

    if request.name is not None:
        existing.name = request.name
    if request.description is not None:
        existing.description = request.description
    if request.status is not None:
        existing.status = WorkflowStatus(request.status)
    if request.steps is not None:
        existing.steps = [WorkflowStep(**s) for s in request.steps]
    if request.tags is not None:
        existing.tags = request.tags

    existing.version += 1
    storage.save_workflow(existing)
    return {"message": "Workflow updated", "version": existing.version}


@router.delete("/{workflow_id}")
async def delete_workflow(workflow_id: str) -> dict:
    """Delete a workflow and all its runs."""
    storage = _get_storage()
    if not storage.delete_workflow(workflow_id):
        raise HTTPException(status_code=404, detail="Workflow not found")
    return {"message": "Workflow deleted"}


@router.post("/{workflow_id}/validate")
async def validate_workflow(workflow_id: str) -> dict:
    """Validate a workflow DAG structure."""
    storage = _get_storage()
    workflow = storage.get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")

    errors = workflow.validate_dag()
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "step_count": len(workflow.steps),
    }


@router.post("/{workflow_id}/duplicate")
async def duplicate_workflow(workflow_id: str) -> dict:
    """Duplicate a workflow."""
    storage = _get_storage()
    workflow = storage.get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")

    copy = workflow.model_copy(
        update={
            "id": "",
            "name": f"{workflow.name} (Copy)",
            "status": WorkflowStatus.DRAFT,
            "version": 1,
            "created_at": None,
            "updated_at": None,
        }
    )
    new_id = storage.save_workflow(copy)
    return {"id": new_id, "message": "Workflow duplicated"}


# ------------------------------------------------------------------
# Execution
# ------------------------------------------------------------------


@router.post("/{workflow_id}/execute")
async def execute_workflow(workflow_id: str, request: ExecuteRequest) -> dict:
    """Execute a workflow immediately."""
    storage = _get_storage()
    engine = _get_engine()

    workflow = storage.get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")

    if workflow.status not in (WorkflowStatus.ACTIVE, WorkflowStatus.DRAFT):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot execute workflow in '{workflow.status.value}' status",
        )

    run = engine.execute(
        workflow,
        initial_context=request.context,
        triggered_by=request.triggered_by,
    )

    return {
        "run_id": run.id,
        "status": run.status.value,
        "steps_executed": len(run.step_results),
        "error": run.error,
    }


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _workflow_summary(w: WorkflowDefinition) -> dict:
    """Return a lightweight summary for list views."""
    return {
        "id": w.id,
        "name": w.name,
        "description": w.description,
        "status": w.status.value,
        "version": w.version,
        "step_count": len(w.steps),
        "tags": w.tags,
        "created_at": w.created_at.isoformat() if w.created_at else None,
        "updated_at": w.updated_at.isoformat() if w.updated_at else None,
    }
