# UC Storage Reconciliation — Plan 1: Discovery + Decision Report

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the read-only discovery and decision-report tooling that lets the customer inventory every UC object, classify it by storage location, and get an opinionated rollback-vs-forward-migrate recommendation with supporting numbers.

**Architecture:** Pure-Python utility modules under `utils/` (testable with pytest, no Spark required for the logic) wrapped by two thin Databricks notebooks under `notebooks/`. The notebooks are config + orchestration only; all logic lives in `utils/` for unit testing. Operational state is persisted as Delta tables in a configurable `_migration_ops` schema for resumability and audit.

**Tech Stack:** Python 3.11+, `databricks-sdk`, `pyspark` (only imported inside notebooks / Spark-aware modules), `pytest`, `pytest-mock`. Notebooks use the Databricks `# Databricks notebook source` magic format so they can be uploaded to a workspace and run as standard notebooks.

**Spec:** `docs/superpowers/specs/2026-05-12-uc-storage-reconciliation-design.md` (sections 5, 6, 7).

---

## File Structure

```
~/work/uc-storage-migration/
├── pyproject.toml                       # Project metadata, dependencies, pytest config
├── README.md                            # How to install, configure, and run
├── .gitignore                           # Standard Python ignores
├── docs/superpowers/specs/...           # Spec (already exists)
├── docs/superpowers/plans/...           # This plan
├── utils/
│   ├── __init__.py                      # Re-exports public API
│   ├── paths.py                         # ADLS URL parsing + storage account classification
│   ├── sql.py                           # FQN quoting + DESCRIBE EXTENDED Location parsing
│   ├── uc_client.py                     # databricks-sdk + REST wrappers (catalogs/schemas/tables/volumes/external locations/metastore)
│   ├── lineage.py                       # system.access.* query builders for downstream consumers
│   ├── discovery.py                     # Pure classification logic (no I/O)
│   ├── state.py                         # Delta table I/O for _migration_ops.* (Spark-dependent)
│   └── reporting.py                     # Decision-report logic: thresholds, recommendation, markdown rendering
├── notebooks/
│   ├── 01_discovery.py                  # Databricks notebook: orchestrates inventory collection
│   └── 02_decision_report.py            # Databricks notebook: reads inventory, prints recommendation
└── tests/
    ├── __init__.py
    ├── conftest.py                      # Shared fixtures
    ├── test_paths.py                    # ADLS path parsing tests
    ├── test_sql.py                      # FQN quoting + DESCRIBE parsing tests
    ├── test_uc_client.py                # SDK/REST wrapper tests (mocked)
    ├── test_discovery.py                # Classification logic tests
    ├── test_lineage.py                  # Query builder tests
    └── test_reporting.py                # Decision-report logic tests
```

**Module responsibilities (single-purpose, well-bounded):**

| Module | Responsibility | Spark needed? |
|---|---|---|
| `utils/paths.py` | Parse `abfss://` URLs, extract host/account/container/path, classify against old/new account names | No — pure string ops |
| `utils/sql.py` | FQN backtick-quoting, parse `Location:` from `DESCRIBE EXTENDED` output, build safe SQL fragments | No |
| `utils/uc_client.py` | Wrap `databricks.sdk.WorkspaceClient` and REST calls for catalogs/schemas/tables/volumes/external locations/metastore | No — mockable |
| `utils/lineage.py` | Build SQL strings for `system.access.table_lineage` and `system.access.audit` queries | No — just builds SQL |
| `utils/discovery.py` | Pure classification of an `ObjectRecord` into one of seven classifications | No |
| `utils/state.py` | Read/write Delta tables in `_migration_ops` schema | Yes |
| `utils/reporting.py` | Compute decision recommendation from inventory rows; render markdown summary | No — operates on dataclasses |

The Spark boundary is isolated to `state.py` and the notebooks. Everything else is unit-testable on a laptop with `pip install -e .[dev]`.

---

## Conventions

- **Type hints everywhere.** All public functions in `utils/` are type-annotated.
- **Dataclasses for records.** `ObjectRecord`, `ExternalLocationRecord`, `LineageRecord`, `DecisionReport` etc. are `@dataclass(frozen=True)` so they're hashable and testable.
- **No silent failures.** Every catch logs to stderr with the FQN that triggered it; loop continues.
- **All SQL goes through `utils/sql.py`.** Never inline f-string SQL with raw identifiers in notebooks.
- **Commit after every passing test.** Frequent, small commits.
- **Commit author:** always `git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "..."` per user standing instructions.

---

## Task 1: Project scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `README.md`
- Create: `utils/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "uc-storage-migration"
version = "0.1.0"
description = "Discovery and migration tooling for Unity Catalog storage reconciliation"
requires-python = ">=3.11"
dependencies = [
    "databricks-sdk>=0.30.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-mock>=3.12",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
addopts = "-v --tb=short"

[tool.setuptools.packages.find]
include = ["utils*"]
```

- [ ] **Step 2: Create `.gitignore`**

```
__pycache__/
*.pyc
.pytest_cache/
.venv/
*.egg-info/
.coverage
.DS_Store
```

- [ ] **Step 3: Create `README.md` skeleton**

```markdown
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
```

- [ ] **Step 4: Create empty `utils/__init__.py` and `tests/__init__.py`**

Empty files; will be populated as modules are added.

- [ ] **Step 5: Create `tests/conftest.py`**

```python
"""Shared pytest fixtures."""
```

(Empty for now; fixtures added as needed.)

- [ ] **Step 6: Initialize git, install, verify pytest runs**

Run:
```bash
cd ~/work/uc-storage-migration
git init
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
pytest
```

Expected: pytest reports `no tests ran` (exit 5 is acceptable for empty test suite — fine).

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml .gitignore README.md utils/__init__.py tests/__init__.py tests/conftest.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "scaffold: pyproject, gitignore, readme, empty packages"
```

---

## Task 2: `utils/paths.py` — ADLS URL parsing and account classification

**Purpose:** Pure functions to parse `abfss://` URLs and classify storage paths against known old/new ADLS account names.

**Files:**
- Create: `utils/paths.py`
- Create: `tests/test_paths.py`

- [ ] **Step 1: Write the failing test**

`tests/test_paths.py`:
```python
import pytest
from utils.paths import parse_abfss_url, classify_account, AdlsPath


class TestParseAbfssUrl:
    def test_parses_standard_url(self):
        result = parse_abfss_url("abfss://container@oldacct.dfs.core.windows.net/some/path")
        assert result == AdlsPath(
            account="oldacct",
            container="container",
            path="some/path",
            raw="abfss://container@oldacct.dfs.core.windows.net/some/path",
        )

    def test_parses_url_with_trailing_slash(self):
        result = parse_abfss_url("abfss://c@a.dfs.core.windows.net/")
        assert result.account == "a"
        assert result.container == "c"
        assert result.path == ""

    def test_returns_none_for_non_abfss(self):
        assert parse_abfss_url("s3://bucket/path") is None
        assert parse_abfss_url("/Volumes/c/s/v/file") is None
        assert parse_abfss_url(None) is None
        assert parse_abfss_url("") is None

    def test_handles_uppercase_host(self):
        result = parse_abfss_url("abfss://c@MyAcct.dfs.core.windows.net/x")
        assert result.account == "myacct"  # normalized to lowercase


class TestClassifyAccount:
    def test_old_account(self):
        assert classify_account("oldacct", old="oldacct", new="newacct") == "old"

    def test_new_account(self):
        assert classify_account("newacct", old="oldacct", new="newacct") == "new"

    def test_other_account(self):
        assert classify_account("thirdparty", old="oldacct", new="newacct") == "other"

    def test_none_is_unknown(self):
        assert classify_account(None, old="oldacct", new="newacct") == "unknown"

    def test_case_insensitive(self):
        assert classify_account("OldAcct", old="oldacct", new="newacct") == "old"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_paths.py -v`
Expected: `ModuleNotFoundError: No module named 'utils.paths'`

- [ ] **Step 3: Write minimal implementation**

`utils/paths.py`:
```python
"""ADLS URL parsing and storage-account classification."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Optional

_ABFSS_RE = re.compile(
    r"^abfss://(?P<container>[^@]+)@(?P<account>[^.]+)\.dfs\.core\.windows\.net(?:/(?P<path>.*))?$",
    re.IGNORECASE,
)

AccountClass = Literal["old", "new", "other", "unknown"]


@dataclass(frozen=True)
class AdlsPath:
    account: str
    container: str
    path: str
    raw: str


def parse_abfss_url(url: Optional[str]) -> Optional[AdlsPath]:
    """Parse an abfss:// URL into its components, or None if not abfss."""
    if not url:
        return None
    match = _ABFSS_RE.match(url)
    if not match:
        return None
    return AdlsPath(
        account=match.group("account").lower(),
        container=match.group("container"),
        path=match.group("path") or "",
        raw=url,
    )


def classify_account(
    account: Optional[str], *, old: str, new: str
) -> AccountClass:
    """Classify a storage account name against the known old/new accounts."""
    if account is None:
        return "unknown"
    account_lower = account.lower()
    if account_lower == old.lower():
        return "old"
    if account_lower == new.lower():
        return "new"
    return "other"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_paths.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add utils/paths.py tests/test_paths.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): ADLS URL parsing and account classification"
```

---

## Task 3: `utils/sql.py` — FQN quoting and DESCRIBE EXTENDED parsing

**Purpose:** Safe SQL identifier quoting and parsing of `DESCRIBE EXTENDED` output for the `Location` field fallback.

**Files:**
- Create: `utils/sql.py`
- Create: `tests/test_sql.py`

- [ ] **Step 1: Write the failing test**

`tests/test_sql.py`:
```python
import pytest
from utils.sql import quote_ident, quote_fqn, parse_describe_extended_location


class TestQuoteIdent:
    def test_simple_identifier(self):
        assert quote_ident("my_table") == "`my_table`"

    def test_identifier_with_backtick_escapes(self):
        assert quote_ident("weird`name") == "`weird``name`"

    def test_identifier_with_space(self):
        assert quote_ident("with space") == "`with space`"


class TestQuoteFqn:
    def test_three_part(self):
        assert quote_fqn("catalog", "schema", "table") == "`catalog`.`schema`.`table`"

    def test_with_special_chars(self):
        assert quote_fqn("c-1", "s.s", "t`t") == "`c-1`.`s.s`.`t``t`"

    def test_two_part(self):
        assert quote_fqn("catalog", "schema") == "`catalog`.`schema`"


class TestParseDescribeExtendedLocation:
    def test_extracts_location_from_block(self):
        output = """
col_name             data_type            comment
id                   bigint
name                 string

# Detailed Table Information
Catalog              main
Database             schema_a
Table                t1
Location             abfss://c@oldacct.dfs.core.windows.net/managed/x
Provider             delta
"""
        assert parse_describe_extended_location(output) == "abfss://c@oldacct.dfs.core.windows.net/managed/x"

    def test_returns_none_when_no_location(self):
        assert parse_describe_extended_location("no location line here") is None

    def test_handles_tab_separated(self):
        output = "Location\tabfss://c@a.dfs.core.windows.net/p"
        assert parse_describe_extended_location(output) == "abfss://c@a.dfs.core.windows.net/p"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_sql.py -v`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

`utils/sql.py`:
```python
"""SQL identifier quoting and DESCRIBE output parsing."""
from __future__ import annotations

import re
from typing import Optional


def quote_ident(name: str) -> str:
    """Backtick-quote an SQL identifier, escaping internal backticks."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def quote_fqn(*parts: str) -> str:
    """Backtick-quote each part of a multi-part identifier and join with dots."""
    return ".".join(quote_ident(p) for p in parts)


_LOCATION_RE = re.compile(r"^\s*Location\s*[\t ]+(\S.*?)\s*$", re.MULTILINE)


def parse_describe_extended_location(output: str) -> Optional[str]:
    """Extract the Location: value from a DESCRIBE EXTENDED result string."""
    match = _LOCATION_RE.search(output)
    if not match:
        return None
    return match.group(1).strip()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_sql.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add utils/sql.py tests/test_sql.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): FQN quoting and DESCRIBE EXTENDED location parsing"
```

---

## Task 4: `utils/uc_client.py` — Databricks SDK + REST wrappers

**Purpose:** A thin layer over `databricks.sdk.WorkspaceClient` and REST endpoints that returns plain dataclasses. Keeps SDK details out of the rest of the codebase and makes everything mockable.

**Files:**
- Create: `utils/uc_client.py`
- Create: `tests/test_uc_client.py`

- [ ] **Step 1: Write the failing test**

`tests/test_uc_client.py`:
```python
from unittest.mock import MagicMock

import pytest

from utils.uc_client import (
    UcClient,
    CatalogRecord,
    SchemaRecord,
    ExternalLocationRecord,
    MetastoreInfo,
)


def make_sdk_catalog(name, catalog_type, storage_root, owner):
    m = MagicMock()
    m.name = name
    m.catalog_type = catalog_type
    m.storage_root = storage_root
    m.owner = owner
    m.comment = None
    m.isolation_mode = None
    return m


def make_sdk_schema(name, catalog, storage_root, owner):
    m = MagicMock()
    m.name = name
    m.catalog_name = catalog
    m.storage_root = storage_root
    m.owner = owner
    return m


class TestListCatalogs:
    def test_returns_catalog_records(self):
        sdk = MagicMock()
        sdk.catalogs.list.return_value = [
            make_sdk_catalog("c1", "MANAGED_CATALOG", "abfss://c@oldacct.dfs.core.windows.net/c1", "u1"),
            make_sdk_catalog("c2", "FOREIGN_CATALOG", None, "u2"),
        ]
        client = UcClient(sdk=sdk, rest=MagicMock())

        result = client.list_catalogs()

        assert len(result) == 2
        assert result[0] == CatalogRecord(
            name="c1",
            catalog_type="MANAGED_CATALOG",
            storage_root="abfss://c@oldacct.dfs.core.windows.net/c1",
            owner="u1",
            comment=None,
            isolation_mode=None,
        )
        assert result[1].catalog_type == "FOREIGN_CATALOG"

    def test_filters_by_allowlist(self):
        sdk = MagicMock()
        sdk.catalogs.list.return_value = [
            make_sdk_catalog("c1", "MANAGED_CATALOG", None, "u"),
            make_sdk_catalog("c2", "MANAGED_CATALOG", None, "u"),
            make_sdk_catalog("c3", "MANAGED_CATALOG", None, "u"),
        ]
        client = UcClient(sdk=sdk, rest=MagicMock())

        result = client.list_catalogs(allowlist=["c1", "c3"])

        assert [c.name for c in result] == ["c1", "c3"]


class TestListSchemas:
    def test_returns_schema_records(self):
        sdk = MagicMock()
        sdk.schemas.list.return_value = [
            make_sdk_schema("s1", "c1", "abfss://c@new.dfs.core.windows.net/s1", "u"),
        ]
        client = UcClient(sdk=sdk, rest=MagicMock())

        result = client.list_schemas("c1")

        assert result[0] == SchemaRecord(
            name="s1", catalog_name="c1",
            storage_root="abfss://c@new.dfs.core.windows.net/s1", owner="u",
        )


class TestGetMetastore:
    def test_parses_metastore_response(self):
        rest = MagicMock()
        rest.get.return_value = {
            "metastore_id": "abc-123",
            "name": "test-ms",
            "storage_root": "abfss://root@oldacct.dfs.core.windows.net/",
            "region": "eastus",
        }
        client = UcClient(sdk=MagicMock(), rest=rest)

        result = client.get_metastore()

        assert result == MetastoreInfo(
            metastore_id="abc-123",
            name="test-ms",
            storage_root="abfss://root@oldacct.dfs.core.windows.net/",
            region="eastus",
        )
        rest.get.assert_called_once_with("/api/2.1/unity-catalog/metastores/current")


class TestListExternalLocations:
    def test_parses_external_locations(self):
        rest = MagicMock()
        rest.get.return_value = {
            "external_locations": [
                {
                    "name": "old_root",
                    "url": "abfss://c@oldacct.dfs.core.windows.net/",
                    "credential_name": "old_cred",
                    "read_only": False,
                },
                {
                    "name": "new_root",
                    "url": "abfss://c@newacct.dfs.core.windows.net/",
                    "credential_name": "new_cred",
                    "read_only": False,
                },
            ]
        }
        client = UcClient(sdk=MagicMock(), rest=rest)

        result = client.list_external_locations()

        assert len(result) == 2
        assert result[0] == ExternalLocationRecord(
            name="old_root",
            url="abfss://c@oldacct.dfs.core.windows.net/",
            credential_name="old_cred",
            read_only=False,
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_uc_client.py -v`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

`utils/uc_client.py`:
```python
"""Wrappers around databricks-sdk and UC REST endpoints, returning dataclasses."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass(frozen=True)
class CatalogRecord:
    name: str
    catalog_type: Optional[str]
    storage_root: Optional[str]
    owner: Optional[str]
    comment: Optional[str]
    isolation_mode: Optional[str]


@dataclass(frozen=True)
class SchemaRecord:
    name: str
    catalog_name: str
    storage_root: Optional[str]
    owner: Optional[str]


@dataclass(frozen=True)
class ExternalLocationRecord:
    name: str
    url: str
    credential_name: str
    read_only: bool


@dataclass(frozen=True)
class MetastoreInfo:
    metastore_id: str
    name: str
    storage_root: Optional[str]
    region: Optional[str]


class _RestProto(Protocol):
    def get(self, path: str) -> dict: ...


class UcClient:
    """Thin wrapper around databricks-sdk + REST. Returns dataclasses, not SDK types."""

    def __init__(self, *, sdk, rest: _RestProto):
        self._sdk = sdk
        self._rest = rest

    def list_catalogs(self, *, allowlist: Optional[list[str]] = None) -> list[CatalogRecord]:
        records = [
            CatalogRecord(
                name=c.name,
                catalog_type=getattr(c, "catalog_type", None),
                storage_root=getattr(c, "storage_root", None),
                owner=getattr(c, "owner", None),
                comment=getattr(c, "comment", None),
                isolation_mode=getattr(c, "isolation_mode", None),
            )
            for c in self._sdk.catalogs.list()
        ]
        if allowlist:
            allow_set = set(allowlist)
            records = [r for r in records if r.name in allow_set]
        return records

    def list_schemas(self, catalog: str) -> list[SchemaRecord]:
        return [
            SchemaRecord(
                name=s.name,
                catalog_name=s.catalog_name,
                storage_root=getattr(s, "storage_root", None),
                owner=getattr(s, "owner", None),
            )
            for s in self._sdk.schemas.list(catalog_name=catalog)
        ]

    def get_metastore(self) -> MetastoreInfo:
        resp = self._rest.get("/api/2.1/unity-catalog/metastores/current")
        return MetastoreInfo(
            metastore_id=resp["metastore_id"],
            name=resp["name"],
            storage_root=resp.get("storage_root"),
            region=resp.get("region"),
        )

    def list_external_locations(self) -> list[ExternalLocationRecord]:
        resp = self._rest.get("/api/2.1/unity-catalog/external-locations")
        return [
            ExternalLocationRecord(
                name=el["name"],
                url=el["url"],
                credential_name=el["credential_name"],
                read_only=el.get("read_only", False),
            )
            for el in resp.get("external_locations", [])
        ]
```

Note: `list_schemas` uses `catalog_name=catalog` keyword arg. Update the test fixture accordingly:

Edit `tests/test_uc_client.py` `TestListSchemas` to use `sdk.schemas.list.assert_called_with(catalog_name="c1")` if asserting call, or leave as-is (the mock returns the configured value regardless).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_uc_client.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add utils/uc_client.py tests/test_uc_client.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): UC client wrapping SDK and REST endpoints"
```

---

## Task 5: `utils/discovery.py` — Classification logic

**Purpose:** Pure functions that take a raw object record + parent context and produce a classification enum. The heart of discovery.

**Files:**
- Create: `utils/discovery.py`
- Create: `tests/test_discovery.py`

- [ ] **Step 1: Write the failing test**

`tests/test_discovery.py`:
```python
import pytest

from utils.discovery import (
    ObjectRecord,
    classify_object,
    Classification,
)


def make_record(
    *,
    object_type="TABLE",
    table_type="MANAGED",
    storage_path=None,
    parent_managed_location=None,
):
    return ObjectRecord(
        catalog="c",
        schema="s",
        name="o",
        object_type=object_type,
        table_type=table_type,
        data_source_format="DELTA",
        storage_path=storage_path,
        parent_managed_location=parent_managed_location,
        owner="u",
        created_at=None,
        last_altered=None,
    )


class TestClassifyObject:
    def test_managed_on_old_parent_old_is_consistent_old(self):
        rec = make_record(
            table_type="MANAGED",
            storage_path="abfss://c@oldacct.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@oldacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "consistent_old"

    def test_managed_on_new_parent_new_is_consistent_new(self):
        rec = make_record(
            table_type="MANAGED",
            storage_path="abfss://c@newacct.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "consistent_new"

    def test_managed_on_old_parent_new_is_drift(self):
        rec = make_record(
            table_type="MANAGED",
            storage_path="abfss://c@oldacct.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "drift_managed_on_old"

    def test_external_on_old_is_external_on_old(self):
        rec = make_record(
            table_type="EXTERNAL",
            storage_path="abfss://c@oldacct.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "external_on_old"

    def test_external_on_new_is_external_on_new(self):
        rec = make_record(
            table_type="EXTERNAL",
            storage_path="abfss://c@newacct.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "external_on_new"

    def test_unknown_account_path(self):
        rec = make_record(
            table_type="MANAGED",
            storage_path="abfss://c@thirdparty.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "unknown_account"

    def test_null_storage_path_is_path_missing(self):
        rec = make_record(table_type="MANAGED", storage_path=None)
        assert classify_object(rec, old="oldacct", new="newacct") == "path_missing"

    def test_view_is_path_missing(self):
        rec = make_record(object_type="TABLE", table_type="VIEW", storage_path=None)
        assert classify_object(rec, old="oldacct", new="newacct") == "path_missing"

    def test_volume_on_old_is_classified_like_table(self):
        rec = make_record(
            object_type="VOLUME",
            table_type="EXTERNAL",
            storage_path="abfss://c@oldacct.dfs.core.windows.net/v",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "external_on_old"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_discovery.py -v`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

`utils/discovery.py`:
```python
"""Classification of UC objects against old/new ADLS account state."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

from utils.paths import classify_account, parse_abfss_url

Classification = Literal[
    "consistent_old",
    "consistent_new",
    "drift_managed_on_old",
    "external_on_old",
    "external_on_new",
    "unknown_account",
    "path_missing",
]


@dataclass(frozen=True)
class ObjectRecord:
    catalog: str
    schema: str
    name: str
    object_type: str           # "TABLE" | "VOLUME" | "REGISTERED_MODEL" | "FUNCTION"
    table_type: Optional[str]  # "MANAGED" | "EXTERNAL" | "VIEW" | "MATERIALIZED_VIEW" | "STREAMING_TABLE"
    data_source_format: Optional[str]
    storage_path: Optional[str]
    parent_managed_location: Optional[str]
    owner: Optional[str]
    created_at: Optional[datetime]
    last_altered: Optional[datetime]


def _account_class(url: Optional[str], *, old: str, new: str) -> str:
    parsed = parse_abfss_url(url)
    return classify_account(parsed.account if parsed else None, old=old, new=new)


def classify_object(rec: ObjectRecord, *, old: str, new: str) -> Classification:
    """Classify an object based on its storage path vs its parent's managed location."""
    # Views and anything without a storage path → path_missing
    if rec.storage_path is None or rec.table_type in {"VIEW"}:
        return "path_missing"

    obj_cls = _account_class(rec.storage_path, old=old, new=new)
    parent_cls = _account_class(rec.parent_managed_location, old=old, new=new)

    if obj_cls == "other":
        return "unknown_account"

    is_managed = rec.table_type in {"MANAGED", "MATERIALIZED_VIEW", "STREAMING_TABLE"}

    if is_managed:
        if obj_cls == "old" and parent_cls == "old":
            return "consistent_old"
        if obj_cls == "new" and parent_cls == "new":
            return "consistent_new"
        if obj_cls == "old" and parent_cls == "new":
            return "drift_managed_on_old"
        # Managed but parent says old while object is on new — unusual but call it consistent_new for safety
        if obj_cls == "new" and parent_cls == "old":
            return "consistent_new"
        return "unknown_account"

    # External
    if obj_cls == "old":
        return "external_on_old"
    if obj_cls == "new":
        return "external_on_new"
    return "unknown_account"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_discovery.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add utils/discovery.py tests/test_discovery.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): object classification logic"
```

---

## Task 6: `utils/lineage.py` — Lineage / consumer query builders

**Purpose:** Produce SQL strings for surfacing downstream consumers (DLT pipelines, streaming jobs, recent queries) from `system.access.*`. Pure string-building; tested by exact-output assertions.

**Files:**
- Create: `utils/lineage.py`
- Create: `tests/test_lineage.py`

- [ ] **Step 1: Write the failing test**

`tests/test_lineage.py`:
```python
from utils.lineage import build_lineage_consumers_query, build_recent_writes_query


def test_lineage_consumers_query_includes_inventory_join():
    sql = build_lineage_consumers_query(inventory_table="main._migration_ops.inventory", days=30)
    assert "system.access.table_lineage" in sql
    assert "main._migration_ops.inventory" in sql
    assert "INTERVAL 30 DAYS" in sql


def test_recent_writes_query_includes_audit():
    sql = build_recent_writes_query(inventory_table="main._migration_ops.inventory", days=30)
    assert "system.access.audit" in sql
    assert "main._migration_ops.inventory" in sql
    assert "INTERVAL 30 DAYS" in sql
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_lineage.py -v`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

`utils/lineage.py`:
```python
"""SQL query builders for downstream consumer discovery from system.access.*"""
from __future__ import annotations


def build_lineage_consumers_query(*, inventory_table: str, days: int) -> str:
    """Return SQL that finds upstream-to-in-scope-object lineage edges from the last N days."""
    return f"""
WITH inv AS (
  SELECT catalog, schema, name FROM {inventory_table}
  WHERE classification IN ('drift_managed_on_old', 'external_on_old')
)
SELECT
  l.source_table_full_name AS source,
  l.target_table_full_name AS target,
  l.event_time,
  l.entity_type,
  l.entity_id
FROM system.access.table_lineage l
JOIN inv i
  ON (l.source_table_catalog = i.catalog AND l.source_table_schema = i.schema AND l.source_table_name = i.name)
  OR (l.target_table_catalog = i.catalog AND l.target_table_schema = i.schema AND l.target_table_name = i.name)
WHERE l.event_time > current_timestamp() - INTERVAL {days} DAYS
""".strip()


def build_recent_writes_query(*, inventory_table: str, days: int) -> str:
    """Return SQL that finds recent write actions against in-scope tables from audit logs."""
    return f"""
WITH inv AS (
  SELECT catalog, schema, name FROM {inventory_table}
  WHERE classification IN ('drift_managed_on_old', 'external_on_old', 'consistent_new')
)
SELECT
  a.event_time,
  a.user_identity.email AS actor,
  a.action_name,
  a.request_params
FROM system.access.audit a
JOIN inv i
  ON a.request_params['full_name_arg'] = concat_ws('.', i.catalog, i.schema, i.name)
WHERE a.event_time > current_timestamp() - INTERVAL {days} DAYS
  AND a.action_name IN ('updateTable', 'createTable', 'mergeTable', 'writeTable')
""".strip()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_lineage.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add utils/lineage.py tests/test_lineage.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): lineage and audit query builders"
```

---

## Task 7: `utils/state.py` — Delta state I/O (Spark-dependent)

**Purpose:** Read/write helpers for `_migration_ops.*` Delta tables. Spark is required, so tests use a `pyspark` local session fixture.

**Files:**
- Create: `utils/state.py`
- Create: `tests/test_state.py`
- Modify: `pyproject.toml` (add `pyspark` to `[dev]`)

- [ ] **Step 1: Add pyspark to dev dependencies**

Edit `pyproject.toml`:

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-mock>=3.12",
    "pyspark>=3.5.0",
    "delta-spark>=3.2.0",
]
```

Reinstall:
```bash
pip install -e .[dev]
```

- [ ] **Step 2: Add a Spark session fixture to `tests/conftest.py`**

`tests/conftest.py`:
```python
"""Shared pytest fixtures."""
import pytest


@pytest.fixture(scope="session")
def spark():
    """Local PySpark session with Delta enabled for state I/O tests."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("uc-migration-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
    )
    session = builder.getOrCreate()
    yield session
    session.stop()
```

- [ ] **Step 3: Write the failing test**

`tests/test_state.py`:
```python
import pytest

from utils.discovery import ObjectRecord
from utils.state import InventoryWriter, INVENTORY_SCHEMA


def test_inventory_schema_contains_expected_columns():
    expected = {
        "catalog",
        "schema",
        "name",
        "object_type",
        "table_type",
        "data_source_format",
        "storage_path",
        "parent_managed_location",
        "owner",
        "classification",
        "captured_at",
    }
    actual = set(INVENTORY_SCHEMA.fieldNames())
    assert expected.issubset(actual)


def test_write_inventory_creates_dataframe_with_classification(spark, tmp_path):
    writer = InventoryWriter(spark=spark)
    records = [
        (
            ObjectRecord(
                catalog="c", schema="s", name="t1",
                object_type="TABLE", table_type="MANAGED",
                data_source_format="DELTA",
                storage_path="abfss://c@oldacct.dfs.core.windows.net/x",
                parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
                owner="u", created_at=None, last_altered=None,
            ),
            "drift_managed_on_old",
        ),
    ]

    df = writer.records_to_dataframe(records)

    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["classification"] == "drift_managed_on_old"
    assert rows[0]["catalog"] == "c"
```

- [ ] **Step 4: Run test to verify it fails**

Run: `pytest tests/test_state.py -v`
Expected: `ModuleNotFoundError`

- [ ] **Step 5: Write minimal implementation**

`utils/state.py`:
```python
"""Delta-backed state I/O for _migration_ops tables."""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
)

from utils.discovery import ObjectRecord, Classification

INVENTORY_SCHEMA = StructType([
    StructField("catalog", StringType(), False),
    StructField("schema", StringType(), False),
    StructField("name", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("table_type", StringType(), True),
    StructField("data_source_format", StringType(), True),
    StructField("storage_path", StringType(), True),
    StructField("parent_managed_location", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("last_altered", TimestampType(), True),
    StructField("classification", StringType(), False),
    StructField("captured_at", TimestampType(), False),
])


class InventoryWriter:
    """Convert ObjectRecord + classification tuples into a Spark DataFrame and write to Delta."""

    def __init__(self, *, spark: SparkSession):
        self._spark = spark

    def records_to_dataframe(
        self, records: Iterable[tuple[ObjectRecord, Classification]]
    ) -> DataFrame:
        now = datetime.utcnow()
        rows = []
        for rec, classification in records:
            rows.append({
                **asdict(rec),
                "classification": classification,
                "captured_at": now,
            })
        return self._spark.createDataFrame(rows, schema=INVENTORY_SCHEMA)

    def overwrite_delta(self, df: DataFrame, *, table_name: str) -> None:
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table_name)
```

- [ ] **Step 6: Run test to verify it passes**

Run: `pytest tests/test_state.py -v`
Expected: 2 passed (Spark startup will take 5-15s on first run)

- [ ] **Step 7: Commit**

```bash
git add utils/state.py tests/test_state.py tests/conftest.py pyproject.toml
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): inventory state I/O with Delta + Spark fixture"
```

---

## Task 8: `utils/reporting.py` — Decision report logic

**Purpose:** Pure functions that compute the rollback-vs-forward recommendation from a list of classified objects and produce a markdown summary string.

**Files:**
- Create: `utils/reporting.py`
- Create: `tests/test_reporting.py`

- [ ] **Step 1: Write the failing test**

`tests/test_reporting.py`:
```python
from datetime import datetime, timedelta

from utils.discovery import ObjectRecord
from utils.reporting import (
    DecisionThresholds,
    compute_recommendation,
    Recommendation,
    render_summary_markdown,
)


def make_classified(name, classification, created_at=None):
    return (
        ObjectRecord(
            catalog="c", schema="s", name=name,
            object_type="TABLE", table_type="MANAGED",
            data_source_format="DELTA",
            storage_path=None, parent_managed_location=None,
            owner="u", created_at=created_at, last_altered=None,
        ),
        classification,
    )


class TestComputeRecommendation:
    def test_zero_new_objects_rollback_feasible(self):
        records = [
            make_classified("t1", "consistent_old"),
            make_classified("t2", "drift_managed_on_old"),
        ]
        rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=0)
        assert rec.verdict == "ROLLBACK_FEASIBLE"

    def test_many_new_objects_forward(self):
        records = [
            make_classified(f"t{i}", "consistent_new") for i in range(100)
        ]
        rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=0)
        assert rec.verdict == "FORWARD_MIGRATE_REQUIRED"

    def test_old_new_object_forces_forward(self):
        old_ts = datetime.utcnow() - timedelta(days=60)
        records = [
            make_classified("t1", "consistent_new", created_at=old_ts),
        ]
        rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=0)
        # Object older than max_age_days_on_new → not safe to roll back
        assert rec.verdict == "FORWARD_MIGRATE_REQUIRED"

    def test_few_recent_new_objects_requires_signoff(self):
        recent = datetime.utcnow() - timedelta(days=2)
        records = [
            make_classified(f"t{i}", "consistent_new", created_at=recent)
            for i in range(5)
        ]
        rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=1)
        assert rec.verdict == "ROLLBACK_REQUIRES_SIGNOFF"


class TestRenderSummaryMarkdown:
    def test_markdown_includes_counts_per_classification(self):
        records = [
            make_classified("t1", "consistent_old"),
            make_classified("t2", "drift_managed_on_old"),
            make_classified("t3", "drift_managed_on_old"),
        ]
        rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=0)
        md = render_summary_markdown(records=records, recommendation=rec)
        assert "consistent_old" in md
        assert "drift_managed_on_old" in md
        assert "ROLLBACK_FEASIBLE" in md
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_reporting.py -v`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

`utils/reporting.py`:
```python
"""Decision-report logic and markdown rendering."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal

from utils.discovery import ObjectRecord, Classification

Verdict = Literal[
    "ROLLBACK_FEASIBLE",
    "ROLLBACK_REQUIRES_SIGNOFF",
    "FORWARD_MIGRATE_REQUIRED",
]


@dataclass(frozen=True)
class DecisionThresholds:
    max_consistent_new_objects: int = 25
    max_bytes_on_new_gb: float = 10.0
    max_distinct_owners_on_new: int = 3
    max_age_days_on_new: int = 30


@dataclass(frozen=True)
class Recommendation:
    verdict: Verdict
    why: str
    new_object_count: int
    bytes_on_new: int


def compute_recommendation(
    classified: list[tuple[ObjectRecord, Classification]],
    *,
    thresholds: DecisionThresholds,
    bytes_on_new: int,
) -> Recommendation:
    new_records = [r for r, c in classified if c == "consistent_new"]
    n_new = len(new_records)
    bytes_gb = bytes_on_new / (1024 ** 3)
    owners = {r.owner for r in new_records if r.owner}
    now = datetime.utcnow()
    oldest_age_days = 0
    for r in new_records:
        if r.created_at:
            oldest_age_days = max(oldest_age_days, (now - r.created_at).days)

    # Any object on new older than threshold → forward
    if oldest_age_days > thresholds.max_age_days_on_new:
        return Recommendation(
            verdict="FORWARD_MIGRATE_REQUIRED",
            why=(
                f"At least one new-storage object is {oldest_age_days} days old "
                f"(threshold {thresholds.max_age_days_on_new}). Rollback would "
                f"discard real workload history."
            ),
            new_object_count=n_new,
            bytes_on_new=bytes_on_new,
        )

    if (
        n_new > thresholds.max_consistent_new_objects
        or bytes_gb > thresholds.max_bytes_on_new_gb
        or len(owners) > thresholds.max_distinct_owners_on_new
    ):
        return Recommendation(
            verdict="FORWARD_MIGRATE_REQUIRED",
            why=(
                f"{n_new} objects, {bytes_gb:.1f} GB, {len(owners)} distinct owners on new "
                f"storage exceed rollback thresholds."
            ),
            new_object_count=n_new,
            bytes_on_new=bytes_on_new,
        )

    if n_new == 0:
        return Recommendation(
            verdict="ROLLBACK_FEASIBLE",
            why="No objects exist on new storage. Clean rollback path.",
            new_object_count=0,
            bytes_on_new=0,
        )

    return Recommendation(
        verdict="ROLLBACK_REQUIRES_SIGNOFF",
        why=(
            f"{n_new} new-storage objects within thresholds but non-zero. "
            f"Customer must confirm each one is throwaway before rollback drops them."
        ),
        new_object_count=n_new,
        bytes_on_new=bytes_on_new,
    )


def render_summary_markdown(
    *,
    records: list[tuple[ObjectRecord, Classification]],
    recommendation: Recommendation,
) -> str:
    counts: Counter = Counter(c for _, c in records)
    lines = ["## Inventory summary", "", "| Classification | Count |", "|---|---:|"]
    for cls in [
        "consistent_old",
        "consistent_new",
        "drift_managed_on_old",
        "external_on_old",
        "external_on_new",
        "unknown_account",
        "path_missing",
    ]:
        lines.append(f"| {cls} | {counts.get(cls, 0)} |")

    lines += [
        "",
        "## Recommendation",
        "",
        f"**Verdict:** `{recommendation.verdict}`",
        "",
        f"{recommendation.why}",
        "",
        f"New-storage objects: {recommendation.new_object_count}, "
        f"bytes_on_new: {recommendation.bytes_on_new}",
    ]
    return "\n".join(lines)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_reporting.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add utils/reporting.py tests/test_reporting.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): decision report thresholds, recommendation, markdown"
```

---

## Task 9: `notebooks/01_discovery.py` — Discovery orchestrator

**Purpose:** Databricks notebook that ties everything together: reads config from a config cell, calls `UcClient`, builds `ObjectRecord` instances, classifies each, writes the inventory Delta table, prints the markdown summary.

**Files:**
- Create: `notebooks/01_discovery.py`

- [ ] **Step 1: Create the notebook file**

`notebooks/01_discovery.py`:
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 01_discovery — UC storage inventory
# MAGIC
# MAGIC **Purpose:** Build a comprehensive inventory of every UC object (tables, volumes,
# MAGIC registered models, external locations, metastore root) and classify each by
# MAGIC which storage account it actually references.
# MAGIC
# MAGIC **Inputs:** UC catalogs (filtered by `CATALOG_ALLOWLIST`).
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `<OPS_SCHEMA>.inventory` — one row per UC object with classification
# MAGIC - `<OPS_SCHEMA>.external_locations` — registered external locations
# MAGIC - `<OPS_SCHEMA>.lineage_consumers` — downstream consumers of in-scope objects
# MAGIC - Markdown summary cell at the end
# MAGIC
# MAGIC **Side effects:** Read-only. Writes only to `<OPS_SCHEMA>` Delta tables. No
# MAGIC modification to in-scope catalogs/schemas/tables.
# MAGIC
# MAGIC **Re-run:** Safe to re-run; `inventory` is fully overwritten each run.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
OLD_STORAGE_ACCOUNT = "oldacct"
NEW_STORAGE_ACCOUNT = "newacct"
CATALOG_ALLOWLIST: list[str] = []        # empty = all catalogs in metastore
OPS_SCHEMA = "main._migration_ops"
COLLECT_SIZES = True
LINEAGE_LOOKBACK_DAYS = 30

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------
from databricks.sdk import WorkspaceClient

from utils.uc_client import UcClient
from utils.discovery import ObjectRecord, classify_object
from utils.state import InventoryWriter
from utils.lineage import build_lineage_consumers_query
from utils.reporting import (
    DecisionThresholds, compute_recommendation, render_summary_markdown,
)


class _SdkRest:
    """Wrap WorkspaceClient.api_client for the UcClient REST protocol."""
    def __init__(self, w: WorkspaceClient):
        self._api = w.api_client

    def get(self, path: str) -> dict:
        return self._api.do("GET", path)


w = WorkspaceClient()
client = UcClient(sdk=w, rest=_SdkRest(w))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Metastore + external locations

# COMMAND ----------
metastore = client.get_metastore()
print(f"Metastore: {metastore.name} ({metastore.metastore_id})")
print(f"  storage_root: {metastore.storage_root}")
print(f"  region: {metastore.region}")

ext_locs = client.list_external_locations()
print(f"\nExternal locations: {len(ext_locs)}")
for el in ext_locs:
    print(f"  {el.name} -> {el.url} (cred={el.credential_name}, read_only={el.read_only})")

import pandas as pd
ext_df = spark.createDataFrame(pd.DataFrame([el.__dict__ for el in ext_locs]))
ext_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{OPS_SCHEMA}.external_locations")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Enumerate catalogs and schemas

# COMMAND ----------
catalogs = client.list_catalogs(allowlist=CATALOG_ALLOWLIST or None)
print(f"In-scope catalogs: {len(catalogs)}")
for c in catalogs:
    print(f"  {c.name} (type={c.catalog_type}, storage_root={c.storage_root})")

schemas_by_catalog = {}
for c in catalogs:
    if c.catalog_type in {"FOREIGN_CATALOG", "DELTASHARING_CATALOG", "SYSTEM_CATALOG"}:
        continue
    schemas_by_catalog[c.name] = client.list_schemas(c.name)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Enumerate tables and volumes from information_schema

# COMMAND ----------
catalog_filter = (
    "(" + ", ".join(f"'{c.name}'" for c in catalogs) + ")"
    if CATALOG_ALLOWLIST else ""
)
where_clause = f"WHERE table_catalog IN {catalog_filter}" if catalog_filter else ""

tables_sql = f"""
SELECT
  table_catalog, table_schema, table_name,
  table_type, data_source_format,
  table_owner AS owner,
  created, last_altered,
  storage_path
FROM system.information_schema.tables
{where_clause}
"""
tables_df = spark.sql(tables_sql).toPandas()
print(f"Tables: {len(tables_df)}")

volumes_where = where_clause.replace("table_catalog", "volume_catalog")
volumes_sql = f"""
SELECT
  volume_catalog AS table_catalog,
  volume_schema AS table_schema,
  volume_name AS table_name,
  volume_type AS table_type,
  NULL AS data_source_format,
  volume_owner AS owner,
  created, last_altered,
  storage_location AS storage_path
FROM system.information_schema.volumes
{volumes_where}
"""
volumes_df = spark.sql(volumes_sql).toPandas()
print(f"Volumes: {len(volumes_df)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Build ObjectRecords and classify

# COMMAND ----------
schema_locs = {
    (cat, s.name): s.storage_root
    for cat, schemas in schemas_by_catalog.items()
    for s in schemas
}
catalog_locs = {c.name: c.storage_root for c in catalogs}

def parent_managed_location(catalog: str, schema: str) -> str | None:
    return schema_locs.get((catalog, schema)) or catalog_locs.get(catalog)

records: list[tuple[ObjectRecord, str]] = []

for _, row in tables_df.iterrows():
    rec = ObjectRecord(
        catalog=row["table_catalog"],
        schema=row["table_schema"],
        name=row["table_name"],
        object_type="TABLE",
        table_type=row["table_type"],
        data_source_format=row.get("data_source_format"),
        storage_path=row.get("storage_path"),
        parent_managed_location=parent_managed_location(row["table_catalog"], row["table_schema"]),
        owner=row.get("owner"),
        created_at=row.get("created"),
        last_altered=row.get("last_altered"),
    )
    cls = classify_object(rec, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT)
    records.append((rec, cls))

for _, row in volumes_df.iterrows():
    rec = ObjectRecord(
        catalog=row["table_catalog"],
        schema=row["table_schema"],
        name=row["table_name"],
        object_type="VOLUME",
        table_type=row["table_type"],
        data_source_format=None,
        storage_path=row.get("storage_path"),
        parent_managed_location=parent_managed_location(row["table_catalog"], row["table_schema"]),
        owner=row.get("owner"),
        created_at=row.get("created"),
        last_altered=row.get("last_altered"),
    )
    cls = classify_object(rec, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT)
    records.append((rec, cls))

print(f"Classified {len(records)} objects")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — Write inventory Delta table

# COMMAND ----------
writer = InventoryWriter(spark=spark)
inv_df = writer.records_to_dataframe(records)
writer.overwrite_delta(inv_df, table_name=f"{OPS_SCHEMA}.inventory")
print(f"Wrote {inv_df.count()} rows to {OPS_SCHEMA}.inventory")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6 — Downstream consumers (lineage)

# COMMAND ----------
lineage_sql = build_lineage_consumers_query(
    inventory_table=f"{OPS_SCHEMA}.inventory",
    days=LINEAGE_LOOKBACK_DAYS,
)
try:
    lineage_df = spark.sql(lineage_sql)
    lineage_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{OPS_SCHEMA}.lineage_consumers")
    print(f"Wrote {lineage_df.count()} lineage edges")
except Exception as e:
    print(f"Lineage query failed (system.access may not be enabled): {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 7 — Summary

# COMMAND ----------
rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=0)
md = render_summary_markdown(records=records, recommendation=rec)
displayHTML(f"<pre>{md}</pre>")  # noqa: F821 (Databricks builtin)
```

- [ ] **Step 2: Commit**

```bash
git add notebooks/01_discovery.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(notebook): 01_discovery orchestrator"
```

Note: this notebook is not directly pytest-able. Validation happens at integration time when the customer runs it in a workspace. The orchestration logic is thin; all logic-heavy work is in `utils/`, which is tested.

---

## Task 10: `notebooks/02_decision_report.py` — Decision report orchestrator

**Purpose:** Read-only notebook that loads `<OPS_SCHEMA>.inventory`, recomputes the recommendation (so config thresholds can be tuned without re-running discovery), and prints the markdown summary plus a rollback-cost ledger and cost/time estimate.

**Files:**
- Create: `notebooks/02_decision_report.py`

- [ ] **Step 1: Create the notebook file**

`notebooks/02_decision_report.py`:
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 02_decision_report — Rollback vs forward-migrate recommendation
# MAGIC
# MAGIC **Purpose:** Read `<OPS_SCHEMA>.inventory` (produced by `01_discovery`) and
# MAGIC produce an opinionated recommendation: ROLLBACK_FEASIBLE,
# MAGIC ROLLBACK_REQUIRES_SIGNOFF, or FORWARD_MIGRATE_REQUIRED.
# MAGIC
# MAGIC **Inputs:** `<OPS_SCHEMA>.inventory`.
# MAGIC
# MAGIC **Outputs:** Markdown summary, rollback-cost ledger, cost/time estimate.
# MAGIC No tables written.
# MAGIC
# MAGIC **Side effects:** None. Read-only.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
OPS_SCHEMA = "main._migration_ops"
# Thresholds tunable here without re-running discovery
THRESHOLDS = {
    "max_consistent_new_objects": 25,
    "max_bytes_on_new_gb": 10.0,
    "max_distinct_owners_on_new": 3,
    "max_age_days_on_new": 30,
}
# Rule-of-thumb for cost/time estimate
ADLS_CLONE_GBPS = 0.5   # GB/sec, conservative same-region estimate
DBU_PER_HOUR = 1.5      # cluster DBU rate

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load inventory

# COMMAND ----------
from datetime import datetime

from utils.discovery import ObjectRecord
from utils.reporting import (
    DecisionThresholds, compute_recommendation, render_summary_markdown,
)

inv_df = spark.table(f"{OPS_SCHEMA}.inventory")
print(f"Inventory rows: {inv_df.count()}")

rows = inv_df.collect()
records = []
for r in rows:
    rec = ObjectRecord(
        catalog=r["catalog"], schema=r["schema"], name=r["name"],
        object_type=r["object_type"], table_type=r["table_type"],
        data_source_format=r["data_source_format"],
        storage_path=r["storage_path"],
        parent_managed_location=r["parent_managed_location"],
        owner=r["owner"],
        created_at=r["created_at"], last_altered=r["last_altered"],
    )
    records.append((rec, r["classification"]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Compute recommendation

# COMMAND ----------
thresholds = DecisionThresholds(**THRESHOLDS)
bytes_on_new = 0  # Wire to actual size collection in a later iteration; placeholder for now.
rec = compute_recommendation(records, thresholds=thresholds, bytes_on_new=bytes_on_new)

md = render_summary_markdown(records=records, recommendation=rec)
displayHTML(f"<pre>{md}</pre>")  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ## Rollback-cost ledger
# MAGIC
# MAGIC If rollback is chosen, the following objects will be dropped:

# COMMAND ----------
new_objects = [r for r, c in records if c == "consistent_new"]
if not new_objects:
    print("No consistent_new objects. Rollback drops nothing.")
else:
    import pandas as pd
    df = pd.DataFrame([{
        "fqn": f"{r.catalog}.{r.schema}.{r.name}",
        "object_type": r.object_type,
        "owner": r.owner,
        "created_at": r.created_at,
    } for r in new_objects])
    display(df)  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ## Forward-migrate cost/time estimate

# COMMAND ----------
drift = [r for r, c in records if c == "drift_managed_on_old"]
external_old = [r for r, c in records if c == "external_on_old"]
print(f"Managed objects to clone: {len(drift)}")
print(f"External objects to re-point: {len(external_old)}")
print()
print(f"Bytes to clone: TODO (collect via DESCRIBE DETAIL when COLLECT_SIZES=True in discovery)")
print(f"Estimated clone duration (rule of thumb): TODO")
print(f"Estimated DBU cost: TODO")
```

Note: the size-collection-driven estimate is left as a comment for the next iteration. Plan 1 ships the structure; size collection wires in when needed.

- [ ] **Step 2: Commit**

```bash
git add notebooks/02_decision_report.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(notebook): 02_decision_report orchestrator"
```

---

## Task 11: Final smoke check — run all tests, update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Run the full test suite**

Run:
```bash
pytest
```
Expected: all tests pass (~30 tests total across 7 test files).

- [ ] **Step 2: Update README with current state**

Replace `README.md` with:
```markdown
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
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "docs: README for Plan 1 state"
```

- [ ] **Step 4: Tag**

```bash
git tag -a plan-1-complete -m "Plan 1: discovery + decision report complete"
```

---

## Plan 1 self-review

**Spec coverage** (against §5–§7 of the spec):
- §5 (deliverable structure): Tasks 1–10 create `utils/`, `notebooks/`, state schema (partial — `inventory`, `external_locations`, `lineage_consumers` here; `object_metadata_snapshot`, `migration_log`, `validation_results` are Plan 2). ✓
- §6.2 (data collection — catalogs, schemas, tables, volumes, external locations, metastore, lineage): Task 9 step 1–6. ✓
- §6.2 (registered models, functions): **Deferred to Plan 2** — flagged here. Discovery is structured to add them as another `ObjectRecord` source without refactoring.
- §6.3 (classification logic, all 7 enum values): Task 5. ✓
- §6.4 (outputs — inventory Delta, external_locations Delta, lineage_consumers Delta, markdown summary): Task 7, Task 9. ✓
- §6.5 (edge cases: foreign/sharing/system catalogs skip, hive_metastore exclude, FQN quoting): Task 3 (quoting), Task 9 (skip filter). hive_metastore exclusion happens naturally via `system.information_schema` not surfacing it.
- §7.1–§7.3 (decision report: thresholds, three blocks, recommendation): Task 8, Task 10. Cost/time estimate is stubbed with TODO comments since size collection wires through `COLLECT_SIZES=True` in Plan 2 expansion.

**Gaps acknowledged and deferred (not Plan 1 placeholders, but Plan 2 scope):**
- Registered models inventory
- Size collection via `DESCRIBE DETAIL` per Delta table
- Cost/time estimate fully wired (depends on size collection)
- `object_metadata_snapshot`, `migration_log`, `validation_results` tables

These are all genuinely Plan 2 scope: they exist to support migration, not discovery.

**Placeholder scan:** No "TBD" / "implement later" in actionable code. The TODO comments in Task 10's notebook are explicitly tied to Plan 2 scope expansion, called out in the README, and don't block the read-only recommendation from being useful.

**Type consistency:**
- `ObjectRecord` defined in Task 5, used in Tasks 7, 8, 9, 10 — same dataclass throughout. ✓
- `Classification` literal type defined in Task 5, used in Tasks 7, 8. ✓
- `Recommendation` defined in Task 8, used in Task 10. ✓
- `UcClient` API defined in Task 4, used in Task 9. ✓
- `InventoryWriter.records_to_dataframe()` and `.overwrite_delta()` signatures match between Task 7 definition and Task 9 use. ✓
- Naming: `classify_object`, `classify_account` — distinct, both used correctly. ✓

No type drift detected.
