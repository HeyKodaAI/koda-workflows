"""SQLite persistence for workflow definitions and run history.

Tables:
    workflows      — workflow definitions (JSON-serialized)
    workflow_runs   — execution history with step results
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional

from koda_workflows.models import (
    RunStatus,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowStatus,
)


class WorkflowStorage:
    """SQLite-backed storage for workflow definitions and runs."""

    def __init__(self, db_path: str = "data/workflows.db") -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS workflows (
                id            TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                description   TEXT DEFAULT '',
                status        TEXT DEFAULT 'draft',
                version       INTEGER DEFAULT 1,
                definition    TEXT NOT NULL,  -- full JSON
                tags          TEXT DEFAULT '[]',
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_workflows_status
                ON workflows(status);
            CREATE INDEX IF NOT EXISTS idx_workflows_updated
                ON workflows(updated_at DESC);

            CREATE TABLE IF NOT EXISTS workflow_runs (
                id              TEXT PRIMARY KEY,
                workflow_id     TEXT NOT NULL,
                workflow_version INTEGER DEFAULT 1,
                status          TEXT DEFAULT 'pending',
                step_results    TEXT DEFAULT '[]',
                current_step_id TEXT,
                context         TEXT DEFAULT '{}',
                triggered_by    TEXT DEFAULT 'manual',
                started_at      TEXT,
                completed_at    TEXT,
                error           TEXT,
                FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_runs_workflow
                ON workflow_runs(workflow_id);
            CREATE INDEX IF NOT EXISTS idx_runs_status
                ON workflow_runs(status);
            CREATE INDEX IF NOT EXISTS idx_runs_started
                ON workflow_runs(started_at DESC);
        """)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Workflow CRUD
    # ------------------------------------------------------------------

    def save_workflow(self, workflow: WorkflowDefinition) -> str:
        """Save or update a workflow definition. Returns the workflow ID."""
        now = datetime.now(timezone.utc).isoformat()

        if not workflow.id:
            workflow.id = str(uuid.uuid4())
            workflow.created_at = datetime.now(timezone.utc)
            workflow.updated_at = workflow.created_at
        else:
            workflow.updated_at = datetime.now(timezone.utc)

        # Serialize workflow to JSON manually to ensure proper datetime handling
        dump_dict = workflow.model_dump(mode='json')
        definition_json = json.dumps(dump_dict)

        self._conn.execute(
            """
            INSERT INTO workflows (id, name, description, status, version,
                                   definition, tags, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                status = excluded.status,
                version = excluded.version,
                definition = excluded.definition,
                tags = excluded.tags,
                updated_at = excluded.updated_at
            """,
            (
                workflow.id,
                workflow.name,
                workflow.description,
                workflow.status.value,
                workflow.version,
                definition_json,
                json.dumps(workflow.tags),
                workflow.created_at.isoformat() if workflow.created_at else now,
                now,
            ),
        )
        self._conn.commit()
        return workflow.id

    def get_workflow(self, workflow_id: str) -> Optional[WorkflowDefinition]:
        """Get a workflow by ID."""
        row = self._conn.execute(
            "SELECT definition FROM workflows WHERE id = ?", (workflow_id,)
        ).fetchone()
        if not row:
            return None
        return WorkflowDefinition.model_validate_json(row["definition"])

    def list_workflows(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[WorkflowDefinition]:
        """List workflows with optional status filter."""
        if status:
            rows = self._conn.execute(
                "SELECT definition FROM workflows WHERE status = ? "
                "ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                (status, limit, offset),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT definition FROM workflows "
                "ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [
            WorkflowDefinition.model_validate_json(r["definition"])
            for r in rows
        ]

    def delete_workflow(self, workflow_id: str) -> bool:
        """Delete a workflow and its runs. Returns True if found."""
        cursor = self._conn.execute(
            "DELETE FROM workflows WHERE id = ?", (workflow_id,)
        )
        self._conn.commit()
        return cursor.rowcount > 0

    def count_workflows(self, status: Optional[str] = None) -> int:
        if status:
            row = self._conn.execute(
                "SELECT COUNT(*) as c FROM workflows WHERE status = ?", (status,)
            ).fetchone()
        else:
            row = self._conn.execute("SELECT COUNT(*) as c FROM workflows").fetchone()
        return row["c"]

    # ------------------------------------------------------------------
    # Workflow Runs
    # ------------------------------------------------------------------

    def save_run(self, run: WorkflowRun) -> str:
        """Save or update a workflow run."""
        if not run.id:
            run.id = str(uuid.uuid4())

        self._conn.execute(
            """
            INSERT INTO workflow_runs (id, workflow_id, workflow_version, status,
                                       step_results, current_step_id, context,
                                       triggered_by, started_at, completed_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                step_results = excluded.step_results,
                current_step_id = excluded.current_step_id,
                context = excluded.context,
                completed_at = excluded.completed_at,
                error = excluded.error
            """,
            (
                run.id,
                run.workflow_id,
                run.workflow_version,
                run.status.value,
                json.dumps([sr.model_dump(mode="json") for sr in run.step_results]),
                run.current_step_id,
                json.dumps(run.context),
                run.triggered_by,
                run.started_at.isoformat() if run.started_at else None,
                run.completed_at.isoformat() if run.completed_at else None,
                run.error,
            ),
        )
        self._conn.commit()
        return run.id

    def get_run(self, run_id: str) -> Optional[WorkflowRun]:
        """Get a single run by ID."""
        row = self._conn.execute(
            "SELECT * FROM workflow_runs WHERE id = ?", (run_id,)
        ).fetchone()
        if not row:
            return None
        return self._row_to_run(row)

    def list_runs(
        self,
        workflow_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[WorkflowRun]:
        """List runs with optional filters."""
        clauses: list[str] = []
        params: list = []
        if workflow_id:
            clauses.append("workflow_id = ?")
            params.append(workflow_id)
        if status:
            clauses.append("status = ?")
            params.append(status)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._conn.execute(
            f"SELECT * FROM workflow_runs {where} "
            "ORDER BY started_at DESC NULLS LAST LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
        return [self._row_to_run(r) for r in rows]

    def count_runs(
        self,
        workflow_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> int:
        clauses: list[str] = []
        params: list = []
        if workflow_id:
            clauses.append("workflow_id = ?")
            params.append(workflow_id)
        if status:
            clauses.append("status = ?")
            params.append(status)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        row = self._conn.execute(
            f"SELECT COUNT(*) as c FROM workflow_runs {where}", params
        ).fetchone()
        return row["c"]

    def _row_to_run(self, row: sqlite3.Row) -> WorkflowRun:
        from koda_workflows.models import StepResult

        step_results_raw = json.loads(row["step_results"] or "[]")
        return WorkflowRun(
            id=row["id"],
            workflow_id=row["workflow_id"],
            workflow_version=row["workflow_version"],
            status=RunStatus(row["status"]),
            step_results=[StepResult(**sr) for sr in step_results_raw],
            current_step_id=row["current_step_id"],
            context=json.loads(row["context"] or "{}"),
            triggered_by=row["triggered_by"],
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
            error=row["error"],
        )

    def close(self) -> None:
        self._conn.close()
