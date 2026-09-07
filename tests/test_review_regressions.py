import pytest
from koda_workflows.engine import WorkflowEngine
from koda_workflows.storage import WorkflowStorage
from koda_workflows.models import WorkflowDefinition, WorkflowStep, StepResult, RunStatus


def step(id, kind="transform", next=(), **config):
    return WorkflowStep(id=id, type=kind, next_steps=list(next), config=config)


def test_unequal_depth_join_waits_for_all_active_inputs():
    storage = WorkflowStorage(":memory:")
    workflow = WorkflowDefinition(name="join", steps=[
        step("start", "trigger", ["a", "b"]), step("a", next=["join"]),
        step("join", operation="template", template="{{late}}", target_field="joined"),
        step("b", next=["c"]), step("c", next=["join"], operation="template", template="ready", target_field="late")])
    storage.save_workflow(workflow)
    run = WorkflowEngine(storage).execute(workflow)
    assert run.status == RunStatus.COMPLETED
    assert run.context["joined"] == "ready"
    assert [r.step_id for r in run.step_results].index("join") > [r.step_id for r in run.step_results].index("c")


class PauseEngine(WorkflowEngine):
    def _execute_step(self, step, context):
        if step.id.startswith("pause"):
            return StepResult(step_id=step.id, status="waiting_approval")
        return super()._execute_step(step, context)


@pytest.mark.parametrize("value,expected", [(True, "yes"), (False, "no")])
def test_resume_keeps_siblings_correct_branch_and_snapshot_after_restart(tmp_path, value, expected):
    path = str(tmp_path / "workflows.db")
    storage = WorkflowStorage(path)
    workflow = WorkflowDefinition(name="resume", steps=[
        step("start", "trigger", ["pause", "sibling"]), step("pause", "action", ["condition"]),
        step("sibling"), step("condition", "condition", field="ok", operator="equals", value=True, true_step="yes", false_step="no"),
        step("yes", next=["join"]), step("no", next=["join"]), step("join")])
    storage.save_workflow(workflow)
    run = PauseEngine(storage).execute(workflow, {"ok": value})
    assert run.status == RunStatus.WAITING_APPROVAL
    workflow.steps = [step("replacement", "trigger")]
    workflow.version += 1
    storage.save_workflow(workflow)
    storage.close()
    reopened = WorkflowStorage(path)
    run = PauseEngine(reopened).resume(run.id)
    ids = [r.step_id for r in run.step_results]
    assert run.status == RunStatus.COMPLETED
    assert "sibling" in ids and "join" in ids and expected in ids
    assert ("no" if expected == "yes" else "yes") not in ids
    assert "replacement" not in ids
    assert ids.count("pause") == 1


def test_repeated_pauses_keep_pending_work():
    storage = WorkflowStorage(":memory:")
    workflow = WorkflowDefinition(name="twice", steps=[step("start", "trigger", ["pause1", "pause2", "last"]),
                                                      step("pause1", "action"), step("pause2", "action"), step("last")])
    storage.save_workflow(workflow)
    engine = PauseEngine(storage)
    run = engine.execute(workflow)
    run = engine.resume(run.id)
    assert run.status == RunStatus.WAITING_APPROVAL
    run = engine.resume(run.id)
    assert run.status == RunStatus.COMPLETED
    assert {r.step_id for r in run.step_results} == {"start", "pause1", "pause2", "last"}


def test_legacy_changed_definition_cannot_resume():
    storage = WorkflowStorage(":memory:")
    workflow = WorkflowDefinition(name="old", steps=[step("start", "trigger", ["pause"]), step("pause", "action")])
    storage.save_workflow(workflow)
    engine = PauseEngine(storage)
    run = engine.execute(workflow)
    run.definition_snapshot = None
    storage.save_run(run)
    workflow.version += 1
    storage.save_workflow(workflow)
    run = engine.resume(run.id)
    assert run.status == RunStatus.FAILED and "changed" in run.error


def test_inactive_branch_descendants_do_not_block_join():
    storage = WorkflowStorage(":memory:")
    workflow = WorkflowDefinition(name="skip", steps=[step("start", "trigger", ["condition"]),
        step("condition", "condition", field="ok", operator="equals", value=True, true_step="a", false_step="b"),
        step("a", next=["join"]), step("b", next=["c"]), step("c", next=["join"]), step("join")])
    storage.save_workflow(workflow)
    run = WorkflowEngine(storage).execute(workflow, {"ok": True})
    assert [r.step_id for r in run.step_results] == ["start", "condition", "a", "join"]
    assert run.status == RunStatus.COMPLETED
