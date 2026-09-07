# Changelog

## 0.1.1 - 2026-09-07

- DAG joins wait for all reachable incoming branches to resolve, and untaken branches are skipped without blocking an active join. (Review 7)
- Execution and resume share condition branch selection, including false branches. (Review 8)
- Pending work is reconstructed from durable step results and an immutable workflow snapshot, preserving siblings through pauses and restarts. The database gains `definition_snapshot`; old paused runs whose definition version changed fail closed. (Review 9)

### Upgrade

The host must authenticate and approve the paused action and complete its side effects before calling `resume()`. Resume marks that paused step completed and continues; it does not grant permission or repeat the action. Newly started runs retain the exact workflow definition even if it is edited while paused. Legacy runs without a snapshot can resume only when the current definition version matches. Action steps still simulate success until the host supplies an executor.

### Validation

37 tests pass, including `tests/test_review_regressions.py`; main README quickstart checked. Tests use synthetic data and isolated databases. No live provider calls or service actions were used.
