# UC Storage Migration

Tooling for discovering and reconciling Unity Catalog object storage locations
after a misordered ADLS migration. See
`docs/superpowers/specs/2026-05-12-uc-storage-reconciliation-design.md` for context.

## Install

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e .[dev]

## Test

    pytest

## Run (in customer workspace)

Upload the contents of `notebooks/` and `utils/` to the workspace, then run
`01_discovery.py` followed by `02_decision_report.py`. See each notebook's
top markdown cell for config and prerequisites.
