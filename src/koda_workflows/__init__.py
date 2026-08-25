"""Workflow builder — visual multi-step automation engine.

Lets users define workflows as directed acyclic graphs of steps,
each step being a trigger, condition, action, or transform.

Architecture:
    WorkflowDefinition → stored as JSON, versioned
    WorkflowEngine     → executes a definition step-by-step
    WorkflowStorage    → SQLite persistence for definitions + run history
    Routes             → CRUD + execute + run history
"""
