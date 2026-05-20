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
2. **Edit `utils/config.py` once.** It is the single source of truth
   for identity values: `OLD_STORAGE_ACCOUNT`, `NEW_STORAGE_ACCOUNT`,
   `OPS_SCHEMA`, `CATALOG_ALLOWLIST`, thresholds, and tunables. Every
   notebook imports from here. Operational gates (`CONFIRMED`,
   `DRY_RUN`, `ACTOR`, `POST_VALIDATION_CLEANUP_OK`) deliberately stay
   per-notebook so a single edit can't arm destructive ops across the
   whole pipeline.
3. **For native UC managed catalogs** (i.e. NOT HMS-federated), run
   `00_repoint_schemas` first. SQL `ALTER SCHEMA SET MANAGED LOCATION`
   is blocked on native UC; this notebook does the equivalent via UC
   REST PATCH and sets up the drift that `01_discovery` then sees.
4. Run notebooks in order: `01_discovery` → `02_decision_report` →
   either `03a_rollback` or `03b_forward_migrate` → `04_validation`.
5. Defaults are `DRY_RUN=True` for the mutating notebooks. Set
   `CONFIRMED=True` and `DRY_RUN=False` only after reviewing the planned
   operations.

### What the migration actually moves

- **Managed Delta tables** (`drift_managed_on_old` classification) —
  `DEEP CLONE` physically reads from OLD and writes fresh files at NEW
  via the schema's repointed `storage_root`. The repo owns the data
  movement end-to-end. No dependency on any prior storage-layer copy.
- **External tables** (`external_on_old`) — `DROP TABLE` + `CREATE
  EXTERNAL TABLE` at the new path. The repo does NOT copy external-table
  data; pre-flight requires the data to already be at NEW (e.g., via
  storage-layer azcopy/rsync). If a prior storage-layer copy is suspect,
  either redo it for these objects, convert them to managed first, or
  exclude them.
- **Managed volumes** — currently deferred (Plan 2.1). Flagged in the
  discovery output for manual handling.
- **Materialized Views / Streaming Tables** — flagged as
  `requires_pipeline_handling`. The repo migrates their backing
  `__materialization_mat_*` Delta tables, but the MV definitions
  themselves need a pipeline-owner `REFRESH` after migration.

## Layout

- `utils/` — pure-Python modules (paths, sql, discovery, reporting,
  governance, migration, validation, preflight, ...)
- `notebooks/` — Databricks notebooks (orchestrators)
- `tests/` — pytest suite for `utils/`
- `docs/` — design spec + implementation plans
