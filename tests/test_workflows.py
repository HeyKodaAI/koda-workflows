"""Tests for the workflow builder — models, storage, engine."""

import os
import tempfile

import pytest

from koda_workflows.models import (
    ConditionOperator,
    RunStatus,
    StepType,
    TriggerType,
    WorkflowDefinition,
    WorkflowStatus,
    WorkflowStep,
)
from koda_workflows.storage import WorkflowStorage
from koda_workflows.engine import WorkflowEngine


@pytest.fixture
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def storage(tmp_db):
    return WorkflowStorage(db_path=tmp_db)


@pytest.fixture
def engine(storage):
    return WorkflowEngine(storage)


def _simple_workflow() -> WorkflowDefinition:
    """A trigger → action workflow."""
    return WorkflowDefinition(
        name="Test Workflow",
        steps=[
            WorkflowStep(
                id="trigger",
                type=StepType.TRIGGER,
                name="Start",
                config={"trigger_type": "manual"},
                next_steps=["action_1"],
            ),
            WorkflowStep(
                id="action_1",
                type=StepType.ACTION,
                name="Send Email",
                config={
                    "action_id": "gmail.send",
                    "service": "gmail",
                    "parameters": {"to": "test@example.com", "subject": "Hello"},
                },
            ),
        ],
    )


def _branching_workflow() -> WorkflowDefinition:
    """Trigger → condition → (action_a | action_b)."""
    return WorkflowDefinition(
        name="Branching",
        steps=[
            WorkflowStep(
                id="trigger",
                type=StepType.TRIGGER,
                config={"trigger_type": "manual"},
                next_steps=["check"],
            ),
            WorkflowStep(
                id="check",
                type=StepType.CONDITION,
                name="Check Priority",
                config={
                    "field": "priority",
                    "operator": "equals",
                    "value": "high",
                    "true_step": "urgent_action",
                    "false_step": "normal_action",
                },
            ),
            WorkflowStep(
                id="urgent_action",
                type=StepType.ACTION,
                name="Urgent Handler",
                config={"action_id": "notify.urgent"},
            ),
            WorkflowStep(
                id="normal_action",
                type=StepType.ACTION,
                name="Normal Handler",
                config={"action_id": "notify.normal"},
            ),
        ],
    )


# ==================================================================
# Model Tests
# ==================================================================


class TestModels:
    def test_workflow_creation(self):
        w = _simple_workflow()
        assert w.name == "Test Workflow"
        assert len(w.steps) == 2
        assert w.status == WorkflowStatus.DRAFT

    def test_trigger_step_property(self):
        w = _simple_workflow()
        assert w.trigger_step is not None
        assert w.trigger_step.id == "trigger"

    def test_step_map(self):
        w = _simple_workflow()
        assert "trigger" in w.step_map
        assert "action_1" in w.step_map

    def test_validate_dag_valid(self):
        w = _simple_workflow()
        errors = w.validate_dag()
        assert errors == []

    def test_validate_dag_no_trigger(self):
        w = WorkflowDefinition(
            name="No trigger",
            steps=[
                WorkflowStep(id="a", type=StepType.ACTION, config={}),
            ],
        )
        errors = w.validate_dag()
        assert any("trigger" in e.lower() for e in errors)

    def test_validate_dag_bad_reference(self):
        w = WorkflowDefinition(
            name="Bad ref",
            steps=[
                WorkflowStep(
                    id="trigger",
                    type=StepType.TRIGGER,
                    config={"trigger_type": "manual"},
                    next_steps=["nonexistent"],
                ),
            ],
        )
        errors = w.validate_dag()
        assert any("nonexistent" in e for e in errors)

    def test_validate_dag_duplicate_ids(self):
        w = WorkflowDefinition(
            name="Dupe",
            steps=[
                WorkflowStep(id="a", type=StepType.TRIGGER, config={}),
                WorkflowStep(id="a", type=StepType.ACTION, config={}),
            ],
        )
        errors = w.validate_dag()
        assert any("duplicate" in e.lower() for e in errors)

    def test_validate_condition_missing_branches(self):
        w = WorkflowDefinition(
            name="Bad condition",
            steps=[
                WorkflowStep(
                    id="trigger",
                    type=StepType.TRIGGER,
                    config={"trigger_type": "manual"},
                    next_steps=["cond"],
                ),
                WorkflowStep(
                    id="cond",
                    type=StepType.CONDITION,
                    config={"field": "x", "operator": "equals", "value": "y"},
                ),
            ],
        )
        errors = w.validate_dag()
        assert any("true_step" in e for e in errors)
        assert any("false_step" in e for e in errors)

    def test_validate_cycle_detection(self):
        w = WorkflowDefinition(
            name="Cycle",
            steps=[
                WorkflowStep(
                    id="trigger",
                    type=StepType.TRIGGER,
                    config={"trigger_type": "manual"},
                    next_steps=["a"],
                ),
                WorkflowStep(id="a", type=StepType.ACTION, config={}, next_steps=["b"]),
                WorkflowStep(id="b", type=StepType.ACTION, config={}, next_steps=["a"]),
            ],
        )
        errors = w.validate_dag()
        assert any("cycle" in e.lower() for e in errors)


# ==================================================================
# Storage Tests
# ==================================================================


class TestStorage:
    def test_save_and_retrieve(self, storage):
        w = _simple_workflow()
        wid = storage.save_workflow(w)
        assert wid
        loaded = storage.get_workflow(wid)
        assert loaded is not None
        assert loaded.name == "Test Workflow"
        assert len(loaded.steps) == 2

    def test_list_workflows(self, storage):
        for i in range(3):
            w = WorkflowDefinition(name=f"WF {i}", steps=[
                WorkflowStep(id="t", type=StepType.TRIGGER, config={})
            ])
            storage.save_workflow(w)
        all_wf = storage.list_workflows()
        assert len(all_wf) == 3

    def test_list_by_status(self, storage):
        w = _simple_workflow()
        w.status = WorkflowStatus.ACTIVE
        storage.save_workflow(w)
        active = storage.list_workflows(status="active")
        draft = storage.list_workflows(status="draft")
        assert len(active) == 1
        assert len(draft) == 0

    def test_update_workflow(self, storage):
        w = _simple_workflow()
        wid = storage.save_workflow(w)
        w.name = "Updated Name"
        w.id = wid
        storage.save_workflow(w)
        loaded = storage.get_workflow(wid)
        assert loaded.name == "Updated Name"

    def test_delete_workflow(self, storage):
        w = _simple_workflow()
        wid = storage.save_workflow(w)
        assert storage.delete_workflow(wid)
        assert storage.get_workflow(wid) is None

    def test_delete_nonexistent(self, storage):
        assert not storage.delete_workflow("nope")

    def test_count_workflows(self, storage):
        assert storage.count_workflows() == 0
        storage.save_workflow(_simple_workflow())
        assert storage.count_workflows() == 1

    def test_save_and_retrieve_run(self, storage):
        from koda_workflows.models import WorkflowRun
        from datetime import datetime, timezone

        w = _simple_workflow()
        wid = storage.save_workflow(w)
        run = WorkflowRun(
            workflow_id=wid,
            status=RunStatus.COMPLETED,
            started_at=datetime.now(timezone.utc),
        )
        rid = storage.save_run(run)
        loaded = storage.get_run(rid)
        assert loaded is not None
        assert loaded.workflow_id == wid
        assert loaded.status == RunStatus.COMPLETED

    def test_list_runs(self, storage):
        from koda_workflows.models import WorkflowRun
        from datetime import datetime, timezone

        w = _simple_workflow()
        wid = storage.save_workflow(w)
        for _ in range(3):
            storage.save_run(WorkflowRun(
                workflow_id=wid,
                started_at=datetime.now(timezone.utc),
            ))
        runs = storage.list_runs(workflow_id=wid)
        assert len(runs) == 3

    def test_count_runs(self, storage):
        from koda_workflows.models import WorkflowRun
        from datetime import datetime, timezone

        w = _simple_workflow()
        wid = storage.save_workflow(w)
        storage.save_run(WorkflowRun(
            workflow_id=wid,
            started_at=datetime.now(timezone.utc),
        ))
        assert storage.count_runs(workflow_id=wid) == 1


# ==================================================================
# Engine Tests
# ==================================================================


class TestEngine:
    def test_execute_simple(self, storage, engine):
        w = _simple_workflow()
        wid = storage.save_workflow(w)
        w = storage.get_workflow(wid)
        run = engine.execute(w)
        assert run.status == RunStatus.COMPLETED
        assert len(run.step_results) == 2
        assert all(sr.status == "completed" for sr in run.step_results)

    def test_execute_with_context(self, storage, engine):
        w = _simple_workflow()
        wid = storage.save_workflow(w)
        w = storage.get_workflow(wid)
        run = engine.execute(w, initial_context={"user": "Mike"})
        assert run.context.get("user") == "Mike"
        assert run.status == RunStatus.COMPLETED

    def test_execute_branching_true(self, storage, engine):
        w = _branching_workflow()
        wid = storage.save_workflow(w)
        w = storage.get_workflow(wid)
        run = engine.execute(w, initial_context={"priority": "high"})
        assert run.status == RunStatus.COMPLETED
        step_ids = [sr.step_id for sr in run.step_results]
        assert "urgent_action" in step_ids
        assert "normal_action" not in step_ids

    def test_execute_branching_false(self, storage, engine):
        w = _branching_workflow()
        wid = storage.save_workflow(w)
        w = storage.get_workflow(wid)
        run = engine.execute(w, initial_context={"priority": "low"})
        assert run.status == RunStatus.COMPLETED
        step_ids = [sr.step_id for sr in run.step_results]
        assert "normal_action" in step_ids
        assert "urgent_action" not in step_ids

    def test_execute_invalid_dag(self, storage, engine):
        w = WorkflowDefinition(name="Invalid", steps=[])
        wid = storage.save_workflow(w)
        w = storage.get_workflow(wid)
        run = engine.execute(w)
        assert run.status == RunStatus.FAILED
        assert "validation" in run.error.lower()

    def test_execute_transform_extract(self, storage, engine):
        w = WorkflowDefinition(
            name="Transform",
            steps=[
                WorkflowStep(
                    id="trigger",
                    type=StepType.TRIGGER,
                    config={"trigger_type": "manual"},
                    next_steps=["transform"],
                ),
                WorkflowStep(
                    id="transform",
                    type=StepType.TRANSFORM,
                    config={
                        "operation": "extract",
                        "source_field": "data.email",
                        "target_field": "recipient",
                    },
                ),
            ],
        )
        wid = storage.save_workflow(w)
        w = storage.get_workflow(wid)
        run = engine.execute(
            w, initial_context={"data": {"email": "mike@test.com"}}
        )
        assert run.status == RunStatus.COMPLETED
        assert run.context.get("recipient") == "mike@test.com"

    def test_execute_transform_template(self, storage, engine):
        w = WorkflowDefinition(
            name="Template",
            steps=[
                WorkflowStep(
                    id="trigger",
                    type=StepType.TRIGGER,
                    config={"trigger_type": "manual"},
                    next_steps=["tmpl"],
                ),
                WorkflowStep(
                    id="tmpl",
                    type=StepType.TRANSFORM,
                    config={
                        "operation": "template",
                        "template": "Hello {{name}}, your order #{{order_id}} is ready!",
                        "target_field": "message",
                    },
                ),
            ],
        )
        wid = storage.save_workflow(w)
        w = storage.get_workflow(wid)
        run = engine.execute(
            w, initial_context={"name": "Mike", "order_id": "12345"}
        )
        assert run.status == RunStatus.COMPLETED
        assert "Mike" in run.context.get("message", "")
        assert "12345" in run.context.get("message", "")

    def test_execute_delay(self, storage, engine):
        w = WorkflowDefinition(
            name="Delay",
            steps=[
                WorkflowStep(
                    id="trigger",
                    type=StepType.TRIGGER,
                    config={"trigger_type": "manual"},
                    next_steps=["wait"],
                ),
                WorkflowStep(
                    id="wait",
                    type=StepType.DELAY,
                    config={"seconds": 0.1},
                ),
            ],
        )
        wid = storage.save_workflow(w)
        w = storage.get_workflow(wid)
        run = engine.execute(w)
        assert run.status == RunStatus.COMPLETED

    def test_interpolate_parameters(self, storage, engine):
        w = WorkflowDefinition(
            name="Interpolate",
            steps=[
                WorkflowStep(
                    id="trigger",
                    type=StepType.TRIGGER,
                    config={"trigger_type": "manual"},
                    next_steps=["send"],
                ),
                WorkflowStep(
                    id="send",
                    type=StepType.ACTION,
                    config={
                        "action_id": "gmail.send",
                        "parameters": {
                            "to": "{{recipient}}",
                            "subject": "Re: {{topic}}",
                        },
                    },
                ),
            ],
        )
        wid = storage.save_workflow(w)
        w = storage.get_workflow(wid)
        run = engine.execute(
            w, initial_context={"recipient": "mike@test.com", "topic": "Koda"}
        )
        assert run.status == RunStatus.COMPLETED
        action_result = run.step_results[1]
        params = action_result.output.get("parameters", {})
        assert params["to"] == "mike@test.com"
        assert params["subject"] == "Re: Koda"

    def test_condition_operators(self, engine):
        """Test various condition operators."""
        assert engine._evaluate_condition("hello", "contains", "ell")
        assert engine._evaluate_condition("hello", "not_contains", "xyz")
        assert engine._evaluate_condition(10, "greater_than", 5)
        assert engine._evaluate_condition(3, "less_than", 10)
        assert engine._evaluate_condition("", "is_empty", None)
        assert engine._evaluate_condition("data", "is_not_empty", None)
        assert engine._evaluate_condition("abc123", "matches_regex", r"\d+")

    def test_resolve_nested_field(self, engine):
        ctx = {"a": {"b": {"c": 42}}}
        assert engine._resolve_field("a.b.c", ctx) == 42
        assert engine._resolve_field("a.b", ctx) == {"c": 42}
        assert engine._resolve_field("a.x", ctx) is None

    def test_run_persisted(self, storage, engine):
        w = _simple_workflow()
        wid = storage.save_workflow(w)
        w = storage.get_workflow(wid)
        run = engine.execute(w)
        loaded = storage.get_run(run.id)
        assert loaded is not None
        assert loaded.status == RunStatus.COMPLETED
        assert len(loaded.step_results) == 2
