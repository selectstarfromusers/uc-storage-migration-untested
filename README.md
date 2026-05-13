# UC Storage Migration

Tooling for discovering and reconciling Unity Catalog object storage locations
after a misordered ADLS migration. See
`docs/superpowers/specs/2026-05-12-uc-storage-reconciliation-design.md` for context.

## Status

**Plan 1 complete:** `01_discovery.py` and `02_decision_report.py` are operational.
`utils/` modules are unit-tested. Migration playbooks (rollback, forward-migrate,
validation) are scheduled for Plan 2.

## Install (local dev)

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e .[dev]

## Test

    pytest

Locally requires `pyspark` and `delta-spark` for `tests/test_state.py`. The
other test modules use only stdlib + `pytest-mock`. On Databricks, pyspark
is preinstalled.

## Run (customer workspace)

1. Upload `utils/` and `notebooks/` to the workspace.
2. Open `notebooks/01_discovery.py`, edit the config cell:
   - `OLD_STORAGE_ACCOUNT`, `NEW_STORAGE_ACCOUNT`
   - `CATALOG_ALLOWLIST` (empty = all catalogs)
   - `OPS_SCHEMA` (default `main._migration_ops`)
3. Run all cells. Result: `<OPS_SCHEMA>.inventory` is written.
4. Open `notebooks/02_decision_report.py`. Run all cells. Result: markdown
   recommendation printed.

## Layout

- `utils/` — pure-Python, pytest-tested modules (paths, sql, discovery, reporting, ...)
- `notebooks/` — Databricks notebooks that orchestrate the utils
- `tests/` — pytest suite for `utils/`
- `docs/` — spec and plans

## Plan 2 (forthcoming)

`03a_rollback.py`, `03b_forward_migrate.py`, `04_validation.py`. Built on the
discovery foundation that Plan 1 ships.
