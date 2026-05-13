# UC Storage Migration

Tooling for discovering and reconciling Unity Catalog object storage locations
after a misordered ADLS migration. See
`docs/superpowers/specs/2026-05-12-uc-storage-reconciliation-design.md` for context.

## Status

**Plan 1 + Plan 2 complete:** discovery, decision report, rollback,
forward-migrate, and four-layer validation all operational. `utils/`
modules carry the testable logic; `notebooks/` are thin Databricks
orchestrators.

## Install (local dev)

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e .[dev]

## Test

    pytest

Locally requires `pyspark` and `delta-spark` for the Spark-dependent test
modules. On Databricks, pyspark is preinstalled.

## Run (customer workspace)

1. Upload `utils/` and `notebooks/` to the workspace, side by side.
2. Run notebooks in order: `01_discovery` → `02_decision_report` →
   either `03a_rollback` or `03b_forward_migrate` → `04_validation`.
3. Each notebook has a config cell. Defaults are `DRY_RUN=True` for the
   mutating notebooks. Set `CONFIRMED=True` and `DRY_RUN=False` only after
   reviewing the planned operations.

## Layout

- `utils/` — pure-Python modules (paths, sql, discovery, reporting,
  governance, migration, validation, preflight, ...)
- `notebooks/` — Databricks notebooks (orchestrators)
- `tests/` — pytest suite for `utils/`
- `docs/` — design spec + implementation plans
