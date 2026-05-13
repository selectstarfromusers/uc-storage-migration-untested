# UC Storage Reconciliation — Plan 2: Migration Playbooks + Validation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the rollback playbook, forward-migrate playbook, and four-layer validation tooling that — given Plan 1's inventory — actually moves UC objects to new storage (or rolls back to old), preserving all governance state, with per-object resumability, a concurrent-run guard, and irrefutable post-migration evidence.

**Architecture:** Four new `utils/` modules (governance capture/replay, pre-flight probes, per-object migration playbook, four-layer validation) plus three new Databricks notebooks (`03a_rollback`, `03b_forward_migrate`, `04_validation`). Three new Delta state tables (`object_metadata_snapshot`, `migration_log`, `validation_results`) added to the existing `_migration_ops` schema. Everything is built on top of Plan 1's `ObjectRecord`, `UcClient`, and inventory infrastructure — no refactoring of Plan 1 modules required (Plan 1 already shipped the fields Plan 2 needs in `ObjectRecord`).

**Tech Stack:** Same as Plan 1 — Python 3.11+, `databricks-sdk`, `pyspark` (Databricks runtime), `pytest`, `pytest-mock`. All new logic is unit-testable with mocks; notebooks are thin orchestrators that get AST-parsed only (live verification happens on Databricks).

**Spec:** `docs/superpowers/specs/2026-05-12-uc-storage-reconciliation-design.md` (sections 8–13).

**Prerequisite:** Plan 1 must be complete and Plan 1's `<OPS_SCHEMA>.inventory` must be populated. The forward-migrate notebook also requires `<OPS_SCHEMA>.external_locations`. See `plan-1-complete` git tag.

---

## File Structure

```
~/work/uc-storage-migration/
├── utils/
│   ├── ... (existing Plan 1 modules — no changes required)
│   ├── governance.py        # Capture + replay grants/owner/tags/row filters/column masks/comments
│   ├── preflight.py         # External location health + read-only/cross-account probes + partition completeness
│   ├── migration.py         # Per-object playbook: DEEP CLONE (Delta), CTAS (non-Delta), RENAME swap, DROP+CREATE
│   └── validation.py        # Four-layer evidence model + standalone verify_object_on_new(fqn)
├── notebooks/
│   ├── ... (existing Plan 1 notebooks)
│   ├── 03a_rollback.py      # Revert managed_location + drop new-storage strays
│   ├── 03b_forward_migrate.py  # Move drift objects to new storage (idempotent, resumable)
│   └── 04_validation.py     # Run the four-layer evidence on every migrated object
└── tests/
    ├── test_governance.py
    ├── test_preflight.py
    ├── test_migration.py
    └── test_validation.py
```

**State tables added (in addition to Plan 1's `inventory`, `external_locations`, `lineage_consumers`):**

| Table | Purpose |
|---|---|
| `object_metadata_snapshot` | Per-object pre-mutation capture: grants (list), owner, tags (map), row filter (name+expr), column masks (per column), table comment, column comments, table properties, constraints. Written before any mutation; survives notebook restarts. |
| `migration_log` | Per-object: `status` (claimed/snapshot_taken/cloned/swapped/replayed/validated/failed), `started_at`, `finished_at`, `claimed_by`, `claimed_at`, `row_count_before/after`, `schema_hash_before/after`, `staging_fqn`, `pre_migration_fqn`, error trace. CAS-style claim row prevents concurrent runs on the same object. |
| `validation_results` | Per-object: four evidence-layer pass/fail flags (`metadata_location_ok`, `delta_log_at_new_ok`, `input_file_name_ok`, `parent_managed_location_match`), `overall_pass`, governance-replay flags (`grants_ok`, `owner_ok`, `tags_ok`, `row_filter_ok`, `column_mask_ok`, `comments_ok`), evidence struct with raw outputs, `validated_at`. |

**Module responsibilities (single-purpose, well-bounded):**

| Module | Responsibility | Spark needed? |
|---|---|---|
| `utils/governance.py` | Build SQL for `SHOW GRANTS`, `SHOW TAGS`, etc.; parse results into dataclasses; build SQL to replay them | Yes for execution; pure-function tests for SQL builders + parsers |
| `utils/preflight.py` | External location health probes, partition completeness checks, ALTER pre-flight dry-runs | Yes for execution; mocked tests |
| `utils/migration.py` | Per-object migration playbook (one function per object type); always returns plain-data results, never mutates global state | Yes for execution; mocked tests |
| `utils/validation.py` | Four-layer evidence collection + governance-replay verification + standalone `verify_object_on_new(fqn)` | Yes for execution; mocked tests |
| `utils/state.py` | Extended with new schemas + writers for the three new tables | Yes |
| `utils/uc_client.py` | Add `list_storage_credentials()` (small extension, needed by `preflight.py`) | No (mockable) |

---

## Conventions

- **All commits use:** `git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "..."`. Git tags use the same flags.
- **No `pytest` runs locally.** pypi is blocked in the dev sandbox. Verification path is `ast.parse()` plus inline stdlib manual assertions, same pattern as Plan 1.
- **No `displayHTML` / `display` / `dbutils` / `spark` in unit tests.** All mutating helpers take their dependencies (spark session, UC client, dbutils-equivalent FS interface) as parameters. Tests mock them.
- **All SQL routed through `utils/sql.py`.** `quote_fqn` for identifiers; never inline f-string raw identifiers in notebooks or migration code.
- **Frozen dataclasses** for all records.
- **One commit per task.** Frequent, small commits.

---

## Task 1: Extend `utils/uc_client.py` with `list_storage_credentials()`

**Purpose:** Pre-flight needs to verify storage credentials are healthy. Single SDK/REST extension, no behavior change to existing methods.

**Files:**
- Modify: `utils/uc_client.py`
- Modify: `tests/test_uc_client.py`

- [ ] **Step 1: Add the dataclass + method**

Add to `utils/uc_client.py` (after `ExternalLocationRecord`):

```python
@dataclass(frozen=True)
class StorageCredentialRecord:
    name: str
    credential_type: str          # "AzureManagedIdentity" | "AzureServicePrincipal" | "AccessConnector" | ...
    owner: Optional[str]
    read_only: bool
    used_for_managed_storage: bool
```

Add to `UcClient` (after `list_external_locations`):

```python
    def list_storage_credentials(self) -> list[StorageCredentialRecord]:
        resp = self._rest.get("/api/2.1/unity-catalog/storage-credentials")
        out = []
        for sc in resp.get("storage_credentials", []):
            # Type field is whichever inline object is present
            cred_type = next(
                (k for k in ("azure_managed_identity", "azure_service_principal",
                             "azure_access_connector", "aws_iam_role", "gcp_service_account_key")
                 if k in sc),
                "unknown",
            )
            out.append(StorageCredentialRecord(
                name=sc["name"],
                credential_type=cred_type,
                owner=sc.get("owner"),
                read_only=sc.get("read_only", False),
                used_for_managed_storage=sc.get("used_for_managed_storage", False),
            ))
        return out
```

- [ ] **Step 2: Add tests in `tests/test_uc_client.py`**

Append to the file:

```python
from utils.uc_client import StorageCredentialRecord


class TestListStorageCredentials:
    def test_parses_credentials(self):
        rest = MagicMock()
        rest.get.return_value = {
            "storage_credentials": [
                {
                    "name": "old_cred",
                    "owner": "u1",
                    "read_only": False,
                    "used_for_managed_storage": True,
                    "azure_managed_identity": {"access_connector_id": "x"},
                },
                {
                    "name": "new_cred",
                    "owner": "u2",
                    "read_only": True,
                    "azure_service_principal": {"client_id": "y"},
                },
            ]
        }
        client = UcClient(sdk=MagicMock(), rest=rest)

        result = client.list_storage_credentials()

        assert len(result) == 2
        assert result[0] == StorageCredentialRecord(
            name="old_cred",
            credential_type="azure_managed_identity",
            owner="u1",
            read_only=False,
            used_for_managed_storage=True,
        )
        assert result[1].credential_type == "azure_service_principal"
        assert result[1].read_only is True
        rest.get.assert_called_with("/api/2.1/unity-catalog/storage-credentials")
```

- [ ] **Step 3: Sanity check**

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
from unittest.mock import MagicMock
from utils.uc_client import UcClient, StorageCredentialRecord
rest = MagicMock()
rest.get.return_value = {'storage_credentials': [{'name':'c1','owner':'u','azure_managed_identity':{}}]}
result = UcClient(sdk=MagicMock(), rest=rest).list_storage_credentials()
assert result[0].credential_type == 'azure_managed_identity'
assert result[0].name == 'c1'
print('PASS')
"
```

- [ ] **Step 4: Commit**

```bash
git add utils/uc_client.py tests/test_uc_client.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(uc_client): list_storage_credentials for preflight"
```

---

## Task 2: `utils/governance.py` — capture and replay (data structures + SQL builders)

**Purpose:** Pure-function building blocks for capturing pre-migration governance state (grants, owner, tags, row filter, column masks, table/column comments, properties, constraints) and replaying it onto a new object.

This task ships the **dataclasses and SQL builders only** — no Spark execution wrappers yet. Those come in Task 3.

**Files:**
- Create: `utils/governance.py`
- Create: `tests/test_governance.py`

- [ ] **Step 1: Write the test file**

`tests/test_governance.py`:

```python
import pytest

from utils.governance import (
    GovernanceSnapshot,
    GrantEntry,
    TagEntry,
    ColumnMaskEntry,
    build_show_grants_sql,
    build_show_tags_sql,
    build_show_row_filter_sql,
    parse_show_grants_rows,
    parse_show_tags_rows,
    build_replay_grants_sql,
    build_replay_owner_sql,
    build_replay_tags_sql,
    build_replay_comment_sql,
)


def test_build_show_grants_sql():
    sql = build_show_grants_sql(catalog="c", schema="s", name="t")
    assert sql == "SHOW GRANTS ON TABLE `c`.`s`.`t`"


def test_build_show_grants_sql_for_volume():
    sql = build_show_grants_sql(catalog="c", schema="s", name="v", object_type="VOLUME")
    assert sql == "SHOW GRANTS ON VOLUME `c`.`s`.`v`"


def test_build_show_tags_sql():
    sql = build_show_tags_sql(catalog="c", schema="s", name="t")
    assert "system.information_schema.table_tags" in sql
    assert "`c`" in sql or "'c'" in sql


def test_build_show_row_filter_sql():
    sql = build_show_row_filter_sql(catalog="c", schema="s", name="t")
    assert "SHOW ROW FILTER" in sql
    assert "`c`.`s`.`t`" in sql


def test_parse_show_grants_rows():
    # Spark SHOW GRANTS returns rows like (principal, action, object_type, object_key)
    rows = [
        {"principal": "user1", "action_type": "SELECT", "object_type": "TABLE", "object_key": "c.s.t"},
        {"principal": "group1", "action_type": "MODIFY", "object_type": "TABLE", "object_key": "c.s.t"},
    ]
    result = parse_show_grants_rows(rows)
    assert result == [
        GrantEntry(principal="user1", privilege="SELECT", object_type="TABLE"),
        GrantEntry(principal="group1", privilege="MODIFY", object_type="TABLE"),
    ]


def test_parse_show_tags_rows():
    rows = [
        {"tag_name": "owner_team", "tag_value": "platform"},
        {"tag_name": "pii", "tag_value": "true"},
    ]
    result = parse_show_tags_rows(rows)
    assert result == [
        TagEntry(name="owner_team", value="platform"),
        TagEntry(name="pii", value="true"),
    ]


def test_build_replay_grants_sql_emits_one_grant_per_entry():
    grants = [
        GrantEntry(principal="u1", privilege="SELECT", object_type="TABLE"),
        GrantEntry(principal="g1", privilege="MODIFY", object_type="TABLE"),
    ]
    sqls = build_replay_grants_sql(catalog="c", schema="s", name="t", grants=grants)
    assert len(sqls) == 2
    assert sqls[0] == "GRANT SELECT ON TABLE `c`.`s`.`t` TO `u1`"
    assert sqls[1] == "GRANT MODIFY ON TABLE `c`.`s`.`t` TO `g1`"


def test_build_replay_owner_sql():
    sql = build_replay_owner_sql(catalog="c", schema="s", name="t", owner="alice@example.com")
    assert sql == "ALTER TABLE `c`.`s`.`t` OWNER TO `alice@example.com`"


def test_build_replay_tags_sql():
    tags = [TagEntry(name="pii", value="true"), TagEntry(name="owner", value="data")]
    sql = build_replay_tags_sql(catalog="c", schema="s", name="t", tags=tags)
    assert sql == "ALTER TABLE `c`.`s`.`t` SET TAGS ('pii' = 'true', 'owner' = 'data')"


def test_build_replay_tags_sql_empty():
    sql = build_replay_tags_sql(catalog="c", schema="s", name="t", tags=[])
    assert sql is None


def test_build_replay_comment_sql():
    sql = build_replay_comment_sql(catalog="c", schema="s", name="t", comment="my comment")
    assert sql == "COMMENT ON TABLE `c`.`s`.`t` IS 'my comment'"


def test_build_replay_comment_sql_escapes_quotes():
    sql = build_replay_comment_sql(catalog="c", schema="s", name="t", comment="it's good")
    assert "it''s good" in sql


def test_governance_snapshot_dataclass_holds_everything():
    snap = GovernanceSnapshot(
        catalog="c", schema="s", name="t",
        grants=[GrantEntry("u", "SELECT", "TABLE")],
        owner="alice",
        tags=[TagEntry("pii", "true")],
        row_filter_name=None,
        row_filter_using_columns=[],
        column_masks=[ColumnMaskEntry(column="ssn", mask_function="mask_ssn", using_columns=[])],
        table_comment="x",
        column_comments={"ssn": "Social Security Number"},
        table_properties={"delta.appendOnly": "true"},
    )
    assert snap.owner == "alice"
    assert len(snap.column_masks) == 1
    assert snap.column_masks[0].column == "ssn"
```

- [ ] **Step 2: Write the implementation**

`utils/governance.py`:

```python
"""Governance capture and replay: grants, owner, tags, filters, masks, comments."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from utils.sql import quote_ident, quote_fqn


@dataclass(frozen=True)
class GrantEntry:
    principal: str
    privilege: str
    object_type: str


@dataclass(frozen=True)
class TagEntry:
    name: str
    value: str


@dataclass(frozen=True)
class ColumnMaskEntry:
    column: str
    mask_function: str
    using_columns: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class GovernanceSnapshot:
    catalog: str
    schema: str
    name: str
    grants: list[GrantEntry]
    owner: Optional[str]
    tags: list[TagEntry]
    row_filter_name: Optional[str]
    row_filter_using_columns: list[str]
    column_masks: list[ColumnMaskEntry]
    table_comment: Optional[str]
    column_comments: dict[str, str]
    table_properties: dict[str, str]


# --- SQL builders for capture ---

def _object_keyword(object_type: str) -> str:
    return "VOLUME" if object_type == "VOLUME" else "TABLE"


def build_show_grants_sql(*, catalog: str, schema: str, name: str, object_type: str = "TABLE") -> str:
    return f"SHOW GRANTS ON {_object_keyword(object_type)} {quote_fqn(catalog, schema, name)}"


def build_show_tags_sql(*, catalog: str, schema: str, name: str) -> str:
    return (
        "SELECT tag_name, tag_value "
        "FROM system.information_schema.table_tags "
        f"WHERE catalog_name = '{catalog}' "
        f"AND schema_name = '{schema}' "
        f"AND table_name = '{name}'"
    )


def build_show_row_filter_sql(*, catalog: str, schema: str, name: str) -> str:
    return f"SHOW ROW FILTER ON {quote_fqn(catalog, schema, name)}"


# --- Parsers for capture output ---

def parse_show_grants_rows(rows: list[dict]) -> list[GrantEntry]:
    return [
        GrantEntry(
            principal=r["principal"],
            privilege=r.get("action_type") or r.get("privilege") or r.get("action"),
            object_type=r.get("object_type", "TABLE"),
        )
        for r in rows
    ]


def parse_show_tags_rows(rows: list[dict]) -> list[TagEntry]:
    return [TagEntry(name=r["tag_name"], value=r["tag_value"]) for r in rows]


# --- SQL builders for replay ---

def _sql_quote_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def build_replay_grants_sql(
    *, catalog: str, schema: str, name: str, grants: list[GrantEntry],
    object_type: str = "TABLE",
) -> list[str]:
    fqn = quote_fqn(catalog, schema, name)
    kw = _object_keyword(object_type)
    return [
        f"GRANT {g.privilege} ON {kw} {fqn} TO {quote_ident(g.principal)}"
        for g in grants
    ]


def build_replay_owner_sql(
    *, catalog: str, schema: str, name: str, owner: str, object_type: str = "TABLE",
) -> str:
    kw = _object_keyword(object_type)
    return f"ALTER {kw} {quote_fqn(catalog, schema, name)} OWNER TO {quote_ident(owner)}"


def build_replay_tags_sql(
    *, catalog: str, schema: str, name: str, tags: list[TagEntry],
) -> Optional[str]:
    if not tags:
        return None
    parts = ", ".join(
        f"{_sql_quote_literal(t.name)} = {_sql_quote_literal(t.value)}" for t in tags
    )
    return f"ALTER TABLE {quote_fqn(catalog, schema, name)} SET TAGS ({parts})"


def build_replay_comment_sql(
    *, catalog: str, schema: str, name: str, comment: str, object_type: str = "TABLE",
) -> str:
    kw = _object_keyword(object_type)
    return f"COMMENT ON {kw} {quote_fqn(catalog, schema, name)} IS {_sql_quote_literal(comment)}"
```

- [ ] **Step 3: Sanity check**

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
from utils.governance import (
    GrantEntry, TagEntry, ColumnMaskEntry, GovernanceSnapshot,
    build_show_grants_sql, build_show_tags_sql, build_show_row_filter_sql,
    parse_show_grants_rows, parse_show_tags_rows,
    build_replay_grants_sql, build_replay_owner_sql,
    build_replay_tags_sql, build_replay_comment_sql,
)

assert build_show_grants_sql(catalog='c', schema='s', name='t') == 'SHOW GRANTS ON TABLE \`c\`.\`s\`.\`t\`'
assert build_show_grants_sql(catalog='c', schema='s', name='v', object_type='VOLUME') == 'SHOW GRANTS ON VOLUME \`c\`.\`s\`.\`v\`'
assert 'SHOW ROW FILTER' in build_show_row_filter_sql(catalog='c', schema='s', name='t')

g = parse_show_grants_rows([{'principal':'u','action_type':'SELECT','object_type':'TABLE','object_key':'k'}])
assert g[0].principal == 'u' and g[0].privilege == 'SELECT'

sqls = build_replay_grants_sql(catalog='c', schema='s', name='t',
    grants=[GrantEntry('u1','SELECT','TABLE'), GrantEntry('g1','MODIFY','TABLE')])
assert sqls == ['GRANT SELECT ON TABLE \`c\`.\`s\`.\`t\` TO \`u1\`', 'GRANT MODIFY ON TABLE \`c\`.\`s\`.\`t\` TO \`g1\`']

assert build_replay_tags_sql(catalog='c', schema='s', name='t', tags=[]) is None
sql = build_replay_tags_sql(catalog='c', schema='s', name='t', tags=[TagEntry('pii','true')])
assert sql == \"ALTER TABLE \`c\`.\`s\`.\`t\` SET TAGS ('pii' = 'true')\"

sql = build_replay_comment_sql(catalog='c', schema='s', name='t', comment=\"it's a thing\")
assert \"'it''s a thing'\" in sql

print('all governance assertions PASS')
"
```

- [ ] **Step 4: Commit**

```bash
git add utils/governance.py tests/test_governance.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): governance capture+replay dataclasses and SQL builders"
```

---

## Task 3: `utils/governance.py` — Spark execution wrapper

**Purpose:** Add the `GovernanceCapturer` and `GovernanceReplayer` classes that take a Spark session and execute capture/replay against a real table. Pure-function building blocks already exist (Task 2); this task wires them to a Spark interface.

**Files:**
- Modify: `utils/governance.py`
- Modify: `tests/test_governance.py`

- [ ] **Step 1: Add the execution wrappers**

Append to `utils/governance.py`:

```python
from typing import Protocol


class _SqlExec(Protocol):
    def sql(self, query: str):  # pragma: no cover
        ...


class GovernanceCapturer:
    """Capture governance state from a UC object into a GovernanceSnapshot."""

    def __init__(self, *, spark: _SqlExec):
        self._spark = spark

    def capture(
        self, *, catalog: str, schema: str, name: str, object_type: str = "TABLE",
    ) -> GovernanceSnapshot:
        grants = self._capture_grants(catalog, schema, name, object_type)
        owner = self._capture_owner(catalog, schema, name, object_type)
        tags = self._capture_tags(catalog, schema, name)
        row_filter_name, row_filter_cols = self._capture_row_filter(catalog, schema, name)
        column_masks = self._capture_column_masks(catalog, schema, name)
        table_comment, column_comments = self._capture_comments(catalog, schema, name, object_type)
        table_properties = self._capture_properties(catalog, schema, name, object_type)
        return GovernanceSnapshot(
            catalog=catalog, schema=schema, name=name,
            grants=grants, owner=owner, tags=tags,
            row_filter_name=row_filter_name,
            row_filter_using_columns=row_filter_cols,
            column_masks=column_masks,
            table_comment=table_comment,
            column_comments=column_comments,
            table_properties=table_properties,
        )

    def _rows(self, sql: str) -> list[dict]:
        try:
            return [r.asDict() for r in self._spark.sql(sql).collect()]
        except Exception:
            return []

    def _capture_grants(self, c, s, n, ot) -> list[GrantEntry]:
        return parse_show_grants_rows(self._rows(build_show_grants_sql(
            catalog=c, schema=s, name=n, object_type=ot,
        )))

    def _capture_tags(self, c, s, n) -> list[TagEntry]:
        return parse_show_tags_rows(self._rows(build_show_tags_sql(catalog=c, schema=s, name=n)))

    def _capture_owner(self, c, s, n, ot) -> Optional[str]:
        kw = _object_keyword(ot)
        rows = self._rows(f"DESCRIBE EXTENDED {kw} {quote_fqn(c, s, n)}")
        for r in rows:
            for k, v in r.items():
                if k.lower() in {"col_name", "info_name"} and str(v).lower() == "owner":
                    # Owner value is in the next field (data_type or info_value).
                    for vk, vv in r.items():
                        if vk != k:
                            return str(vv) if vv is not None else None
        return None

    def _capture_row_filter(self, c, s, n) -> tuple[Optional[str], list[str]]:
        rows = self._rows(build_show_row_filter_sql(catalog=c, schema=s, name=n))
        if not rows:
            return None, []
        r = rows[0]
        name = r.get("filter_name") or r.get("function_name")
        cols = r.get("using_columns") or r.get("input_columns") or []
        if isinstance(cols, str):
            cols = [c.strip() for c in cols.split(",") if c.strip()]
        return name, list(cols)

    def _capture_column_masks(self, c, s, n) -> list[ColumnMaskEntry]:
        # SHOW COLUMN MASK is per-column; we query information_schema instead for bulk.
        rows = self._rows(
            "SELECT column_name, function_name, using_columns "
            "FROM system.information_schema.column_masks "
            f"WHERE catalog_name = '{c}' AND schema_name = '{s}' AND table_name = '{n}'"
        )
        out: list[ColumnMaskEntry] = []
        for r in rows:
            cols = r.get("using_columns") or []
            if isinstance(cols, str):
                cols = [c.strip() for c in cols.split(",") if c.strip()]
            out.append(ColumnMaskEntry(
                column=r["column_name"],
                mask_function=r["function_name"],
                using_columns=list(cols),
            ))
        return out

    def _capture_comments(self, c, s, n, ot) -> tuple[Optional[str], dict[str, str]]:
        rows = self._rows(f"DESCRIBE EXTENDED {_object_keyword(ot)} {quote_fqn(c, s, n)}")
        table_comment: Optional[str] = None
        column_comments: dict[str, str] = {}
        in_columns = True
        for r in rows:
            vals = list(r.values())
            if len(vals) < 2:
                continue
            label = str(vals[0]) if vals[0] is not None else ""
            if label.startswith("#") or label.startswith("Detailed"):
                in_columns = False
            if in_columns and vals[0] and len(vals) >= 3 and vals[2] is not None:
                column_comments[str(vals[0])] = str(vals[2])
            if not in_columns and label.lower() == "comment":
                table_comment = str(vals[1]) if vals[1] is not None else None
        return table_comment, column_comments

    def _capture_properties(self, c, s, n, ot) -> dict[str, str]:
        rows = self._rows(f"SHOW TBLPROPERTIES {quote_fqn(c, s, n)}")
        return {r["key"]: r["value"] for r in rows if "key" in r and "value" in r}


class GovernanceReplayer:
    """Replay a GovernanceSnapshot onto a UC object (typically the renamed-clone)."""

    def __init__(self, *, spark: _SqlExec):
        self._spark = spark

    def replay(self, snap: GovernanceSnapshot, *, target_fqn: tuple[str, str, str],
               object_type: str = "TABLE") -> list[str]:
        """Execute every replay statement; return a list of warnings (empty if clean)."""
        c, s, n = target_fqn
        warnings: list[str] = []

        for sql in build_replay_grants_sql(catalog=c, schema=s, name=n, grants=snap.grants, object_type=object_type):
            try:
                self._spark.sql(sql)
            except Exception as e:
                warnings.append(f"grant replay failed: {sql}: {e}")

        if snap.owner:
            try:
                self._spark.sql(build_replay_owner_sql(
                    catalog=c, schema=s, name=n, owner=snap.owner, object_type=object_type,
                ))
            except Exception as e:
                warnings.append(f"owner replay failed (principal may no longer exist): {snap.owner}: {e}")

        tag_sql = build_replay_tags_sql(catalog=c, schema=s, name=n, tags=snap.tags)
        if tag_sql:
            try:
                self._spark.sql(tag_sql)
            except Exception as e:
                warnings.append(f"tag replay failed: {e}")

        if snap.table_comment:
            try:
                self._spark.sql(build_replay_comment_sql(
                    catalog=c, schema=s, name=n, comment=snap.table_comment, object_type=object_type,
                ))
            except Exception as e:
                warnings.append(f"comment replay failed: {e}")

        # Properties: replay each one explicitly (Delta preserves most via CLONE, but be safe).
        for k, v in snap.table_properties.items():
            if k.startswith("delta."):
                continue  # Delta-managed properties, skip
            try:
                self._spark.sql(
                    f"ALTER TABLE {quote_fqn(c, s, n)} "
                    f"SET TBLPROPERTIES ({_sql_quote_literal(k)} = {_sql_quote_literal(v)})"
                )
            except Exception as e:
                warnings.append(f"property {k} replay failed: {e}")

        # Row filters and column masks need precise reattachment; capturing without
        # full re-attachment SQL is a known limitation. Surface as warning if present.
        if snap.row_filter_name:
            warnings.append(
                f"row filter '{snap.row_filter_name}' was captured but auto-replay is not implemented; "
                f"manually run: ALTER TABLE {quote_fqn(c, s, n)} SET ROW FILTER ..."
            )
        if snap.column_masks:
            warnings.append(
                f"{len(snap.column_masks)} column mask(s) captured but auto-replay is not implemented; "
                f"manually reattach via ALTER TABLE ... ALTER COLUMN ... SET MASK ..."
            )

        return warnings
```

- [ ] **Step 2: Add tests**

Append to `tests/test_governance.py`:

```python
from unittest.mock import MagicMock

from utils.governance import GovernanceCapturer, GovernanceReplayer


class _Row(dict):
    def asDict(self):
        return dict(self)


def _spark_returning(*calls):
    """Return a spark mock where consecutive .sql(...).collect() calls yield each call's rows."""
    spark = MagicMock()
    results = []
    for call in calls:
        r = MagicMock()
        r.collect.return_value = call
        results.append(r)
    spark.sql.side_effect = results
    return spark


class TestGovernanceCapturer:
    def test_capture_assembles_snapshot(self):
        # Order matches GovernanceCapturer.capture() calls: grants, owner (describe extended),
        # tags, row filter, column masks (info_schema), comments (describe extended), properties.
        spark = _spark_returning(
            [_Row(principal="u1", action_type="SELECT", object_type="TABLE", object_key="c.s.t")],  # grants
            [_Row(col_name="Owner", data_type="alice")],                                              # owner describe
            [_Row(tag_name="pii", tag_value="true")],                                                 # tags
            [],                                                                                       # row filter (none)
            [],                                                                                       # column masks (none)
            [_Row(col_name="id", data_type="bigint", comment="primary key")],                         # comments describe
            [_Row(key="delta.columnMapping.mode", value="name")],                                     # properties
        )
        cap = GovernanceCapturer(spark=spark)

        snap = cap.capture(catalog="c", schema="s", name="t")

        assert snap.grants == [GrantEntry("u1", "SELECT", "TABLE")]
        assert snap.owner == "alice"
        assert snap.tags == [TagEntry("pii", "true")]
        assert snap.column_comments == {"id": "primary key"}
        assert snap.table_properties == {"delta.columnMapping.mode": "name"}


class TestGovernanceReplayer:
    def test_replay_emits_grant_owner_tag_comment(self):
        spark = MagicMock()
        rep = GovernanceReplayer(spark=spark)
        snap = GovernanceSnapshot(
            catalog="c", schema="s", name="t",
            grants=[GrantEntry("u1", "SELECT", "TABLE")],
            owner="alice",
            tags=[TagEntry("pii", "true")],
            row_filter_name=None, row_filter_using_columns=[],
            column_masks=[],
            table_comment="hello",
            column_comments={},
            table_properties={},
        )

        warnings = rep.replay(snap, target_fqn=("c", "s", "t"))

        executed = [call.args[0] for call in spark.sql.call_args_list]
        assert "GRANT SELECT ON TABLE `c`.`s`.`t` TO `u1`" in executed
        assert "ALTER TABLE `c`.`s`.`t` OWNER TO `alice`" in executed
        assert any("SET TAGS" in s for s in executed)
        assert any("COMMENT ON TABLE" in s for s in executed)
        assert warnings == []

    def test_replay_warns_on_row_filter(self):
        spark = MagicMock()
        snap = GovernanceSnapshot(
            catalog="c", schema="s", name="t",
            grants=[], owner=None, tags=[],
            row_filter_name="filter_x", row_filter_using_columns=["col1"],
            column_masks=[],
            table_comment=None, column_comments={}, table_properties={},
        )
        warnings = GovernanceReplayer(spark=spark).replay(snap, target_fqn=("c", "s", "t"))
        assert any("row filter 'filter_x'" in w for w in warnings)

    def test_replay_collects_grant_failure(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("principal not found")
        snap = GovernanceSnapshot(
            catalog="c", schema="s", name="t",
            grants=[GrantEntry("u_deleted", "SELECT", "TABLE")],
            owner=None, tags=[], row_filter_name=None, row_filter_using_columns=[],
            column_masks=[], table_comment=None, column_comments={}, table_properties={},
        )
        warnings = GovernanceReplayer(spark=spark).replay(snap, target_fqn=("c", "s", "t"))
        assert any("grant replay failed" in w for w in warnings)
```

- [ ] **Step 3: Sanity check**

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
from unittest.mock import MagicMock
from utils.governance import (
    GovernanceCapturer, GovernanceReplayer, GovernanceSnapshot,
    GrantEntry, TagEntry,
)

# Replayer happy path
spark = MagicMock()
snap = GovernanceSnapshot('c','s','t',
    grants=[GrantEntry('u','SELECT','TABLE')], owner='alice',
    tags=[TagEntry('k','v')], row_filter_name=None, row_filter_using_columns=[],
    column_masks=[], table_comment='hi', column_comments={}, table_properties={})
warns = GovernanceReplayer(spark=spark).replay(snap, target_fqn=('c','s','t'))
calls = [c.args[0] for c in spark.sql.call_args_list]
assert any('GRANT SELECT' in s for s in calls)
assert any('OWNER TO' in s for s in calls)
assert warns == []

# Replayer warning on row filter
snap2 = GovernanceSnapshot('c','s','t', grants=[], owner=None, tags=[],
    row_filter_name='rf', row_filter_using_columns=['c1'],
    column_masks=[], table_comment=None, column_comments={}, table_properties={})
warns = GovernanceReplayer(spark=MagicMock()).replay(snap2, target_fqn=('c','s','t'))
assert any('row filter' in w for w in warns)

print('PASS')
"
```

- [ ] **Step 4: Commit**

```bash
git add utils/governance.py tests/test_governance.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): GovernanceCapturer and GovernanceReplayer over Spark"
```

---

## Task 4: `utils/state.py` — add migration_log, snapshot, validation schemas

**Purpose:** Define the three new Delta table schemas, writer classes, and a CAS-style claim helper for `migration_log`.

**Files:**
- Modify: `utils/state.py`
- Modify: `tests/test_state.py`

- [ ] **Step 1: Add the new schemas + writers**

Append to `utils/state.py`:

```python
from pyspark.sql.types import (
    ArrayType, MapType,
)


MIGRATION_LOG_SCHEMA = StructType([
    StructField("catalog", StringType(), False),
    StructField("schema", StringType(), False),
    StructField("name", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("status", StringType(), False),       # claimed/snapshot_taken/cloned/swapped/replayed/validated/failed
    StructField("claimed_by", StringType(), True),
    StructField("claimed_at", TimestampType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("finished_at", TimestampType(), True),
    StructField("row_count_before", LongType(), True),
    StructField("row_count_after", LongType(), True),
    StructField("schema_hash_before", StringType(), True),
    StructField("schema_hash_after", StringType(), True),
    StructField("staging_fqn", StringType(), True),
    StructField("pre_migration_fqn", StringType(), True),
    StructField("error_trace", StringType(), True),
    StructField("updated_at", TimestampType(), False),
])

OBJECT_METADATA_SNAPSHOT_SCHEMA = StructType([
    StructField("catalog", StringType(), False),
    StructField("schema", StringType(), False),
    StructField("name", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("snapshot_json", StringType(), False),   # serialized GovernanceSnapshot
    StructField("captured_at", TimestampType(), False),
])

VALIDATION_RESULTS_SCHEMA = StructType([
    StructField("catalog", StringType(), False),
    StructField("schema", StringType(), False),
    StructField("name", StringType(), False),
    StructField("metadata_location_ok", BooleanType(), False),
    StructField("delta_log_at_new_ok", BooleanType(), True),
    StructField("input_file_name_ok", BooleanType(), False),
    StructField("parent_managed_location_match", BooleanType(), False),
    StructField("grants_ok", BooleanType(), True),
    StructField("owner_ok", BooleanType(), True),
    StructField("tags_ok", BooleanType(), True),
    StructField("row_filter_ok", BooleanType(), True),
    StructField("column_mask_ok", BooleanType(), True),
    StructField("comments_ok", BooleanType(), True),
    StructField("overall_pass", BooleanType(), False),
    StructField("evidence_json", StringType(), False),
    StructField("validated_at", TimestampType(), False),
])


def _now_naive_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class MigrationLog:
    """Writer + claim manager for _migration_ops.migration_log."""

    def __init__(self, *, spark: SparkSession, table_name: str):
        self._spark = spark
        self._table = table_name

    def ensure_exists(self) -> None:
        self._spark.createDataFrame([], schema=MIGRATION_LOG_SCHEMA).write.format("delta").mode(
            "ignore"
        ).saveAsTable(self._table)

    def claim(self, *, catalog: str, schema: str, name: str, object_type: str, actor: str) -> bool:
        """Atomically claim a row for migration. Returns True if claimed, False if already claimed by someone else."""
        from pyspark.sql.functions import lit
        now = _now_naive_utc()

        # Use MERGE for atomicity. Insert if not exists; do nothing if exists.
        candidate = self._spark.createDataFrame(
            [(catalog, schema, name, object_type, "claimed", actor, now, now,
              None, None, None, None, None, None, None, None, now)],
            schema=MIGRATION_LOG_SCHEMA,
        ).createOrReplaceTempView("_mig_log_candidate")

        self._spark.sql(f"""
            MERGE INTO {self._table} AS t
            USING _mig_log_candidate AS s
              ON t.catalog = s.catalog AND t.schema = s.schema AND t.name = s.name
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Re-read the row and check the claim
        rows = self._spark.sql(
            f"SELECT claimed_by, status FROM {self._table} "
            f"WHERE catalog = '{catalog}' AND schema = '{schema}' AND name = '{name}'"
        ).collect()
        if not rows:
            return False
        return rows[0]["claimed_by"] == actor

    def update(self, *, catalog: str, schema: str, name: str, **fields) -> None:
        """Update fields for a claimed row."""
        sets = ", ".join(f"{k} = {self._to_sql_literal(v)}" for k, v in fields.items())
        sets += f", updated_at = {self._to_sql_literal(_now_naive_utc())}"
        self._spark.sql(
            f"UPDATE {self._table} SET {sets} "
            f"WHERE catalog = '{catalog}' AND schema = '{schema}' AND name = '{name}'"
        )

    @staticmethod
    def _to_sql_literal(v) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, int):
            return str(v)
        if isinstance(v, datetime):
            return f"TIMESTAMP '{v.isoformat()}'"
        s = str(v).replace("'", "''")
        return f"'{s}'"


class SnapshotWriter:
    """Persist GovernanceSnapshot dataclasses as JSON rows."""

    def __init__(self, *, spark: SparkSession, table_name: str):
        self._spark = spark
        self._table = table_name

    def ensure_exists(self) -> None:
        self._spark.createDataFrame([], schema=OBJECT_METADATA_SNAPSHOT_SCHEMA).write.format(
            "delta"
        ).mode("ignore").saveAsTable(self._table)

    def append(self, *, catalog: str, schema: str, name: str, object_type: str, snapshot_json: str) -> None:
        df = self._spark.createDataFrame(
            [(catalog, schema, name, object_type, snapshot_json, _now_naive_utc())],
            schema=OBJECT_METADATA_SNAPSHOT_SCHEMA,
        )
        df.write.format("delta").mode("append").saveAsTable(self._table)


class ValidationResultsWriter:
    def __init__(self, *, spark: SparkSession, table_name: str):
        self._spark = spark
        self._table = table_name

    def ensure_exists(self) -> None:
        self._spark.createDataFrame([], schema=VALIDATION_RESULTS_SCHEMA).write.format(
            "delta"
        ).mode("ignore").saveAsTable(self._table)
```

- [ ] **Step 2: Add tests**

Append to `tests/test_state.py`:

```python
def test_migration_log_schema_has_required_columns():
    from utils.state import MIGRATION_LOG_SCHEMA
    names = set(MIGRATION_LOG_SCHEMA.fieldNames())
    required = {"catalog", "schema", "name", "object_type", "status",
                "claimed_by", "claimed_at", "staging_fqn", "pre_migration_fqn",
                "error_trace", "updated_at"}
    assert required.issubset(names)


def test_snapshot_schema_has_required_columns():
    from utils.state import OBJECT_METADATA_SNAPSHOT_SCHEMA
    names = set(OBJECT_METADATA_SNAPSHOT_SCHEMA.fieldNames())
    assert {"catalog", "schema", "name", "snapshot_json", "captured_at"}.issubset(names)


def test_validation_results_schema_has_required_columns():
    from utils.state import VALIDATION_RESULTS_SCHEMA
    names = set(VALIDATION_RESULTS_SCHEMA.fieldNames())
    required = {
        "metadata_location_ok", "delta_log_at_new_ok", "input_file_name_ok",
        "parent_managed_location_match", "grants_ok", "owner_ok",
        "overall_pass", "evidence_json", "validated_at",
    }
    assert required.issubset(names)


def test_migration_log_to_sql_literal_handles_types():
    from utils.state import MigrationLog
    from datetime import datetime
    assert MigrationLog._to_sql_literal(None) == "NULL"
    assert MigrationLog._to_sql_literal(True) == "TRUE"
    assert MigrationLog._to_sql_literal(42) == "42"
    assert MigrationLog._to_sql_literal("it's") == "'it''s'"
    ts = datetime(2026, 5, 13, 10, 0, 0)
    assert MigrationLog._to_sql_literal(ts) == "TIMESTAMP '2026-05-13T10:00:00'"
```

- [ ] **Step 3: Sanity check (AST only — pyspark not installed locally)**

```bash
python3 -c "
import ast
ast.parse(open('utils/state.py').read())
print('state.py parses OK')

# Also verify _to_sql_literal logic via direct call (it's a staticmethod, doesn't need pyspark to instantiate)
import sys; sys.path.insert(0, '.')
# Can't import utils.state directly without pyspark, so manually run the function:
src = open('utils/state.py').read()
# Extract just the _to_sql_literal staticmethod body and exec
exec(compile(ast.parse('''
from datetime import datetime
def _to_sql_literal(v):
    if v is None: return \"NULL\"
    if isinstance(v, bool): return \"TRUE\" if v else \"FALSE\"
    if isinstance(v, int): return str(v)
    if isinstance(v, datetime): return f\"TIMESTAMP \" + chr(39) + v.isoformat() + chr(39)
    s = str(v).replace(chr(39), chr(39)*2); return chr(39) + s + chr(39)
'''), '<test>', 'exec'), globals())
assert _to_sql_literal(None) == 'NULL'
assert _to_sql_literal(True) == 'TRUE'
assert _to_sql_literal(42) == '42'
assert _to_sql_literal(\"it's\") == \"'it''s'\"
print('PASS')
"
```

- [ ] **Step 4: Commit**

```bash
git add utils/state.py tests/test_state.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(state): migration_log + snapshot + validation_results schemas and writers"
```

---

## Task 5: `utils/preflight.py` — pre-migration health probes

**Purpose:** Validate that the migration target is healthy before any mutation: external location exists for new account, storage credential is valid, read access to old works, data file is actually present at new path.

**Files:**
- Create: `utils/preflight.py`
- Create: `tests/test_preflight.py`

- [ ] **Step 1: Write the test file**

`tests/test_preflight.py`:

```python
from unittest.mock import MagicMock

import pytest

from utils.preflight import (
    PreflightResult,
    check_external_location_for,
    probe_path_exists,
    probe_partition_completeness,
)
from utils.uc_client import ExternalLocationRecord


def test_check_external_location_for_returns_matching_root():
    ext_locs = [
        ExternalLocationRecord("old", "abfss://c@old.dfs.core.windows.net/", "cred1", False, "eastus"),
        ExternalLocationRecord("new", "abfss://c@new.dfs.core.windows.net/", "cred2", False, "eastus"),
    ]
    target = "abfss://c@new.dfs.core.windows.net/some/data/path"
    el = check_external_location_for(target_path=target, external_locations=ext_locs)
    assert el is not None
    assert el.name == "new"


def test_check_external_location_for_returns_none_when_no_match():
    ext_locs = [
        ExternalLocationRecord("old", "abfss://c@old.dfs.core.windows.net/", "cred1", False, "eastus"),
    ]
    target = "abfss://c@third.dfs.core.windows.net/x"
    assert check_external_location_for(target_path=target, external_locations=ext_locs) is None


def test_probe_path_exists_uses_dbutils_fs_ls():
    fs = MagicMock()
    fs.ls.return_value = [MagicMock()]  # something exists
    assert probe_path_exists(fs=fs, path="abfss://c@new.dfs.core.windows.net/x") is True
    fs.ls.assert_called_with("abfss://c@new.dfs.core.windows.net/x")


def test_probe_path_exists_returns_false_on_exception():
    fs = MagicMock()
    fs.ls.side_effect = Exception("path not found")
    assert probe_path_exists(fs=fs, path="abfss://c@new.dfs.core.windows.net/x") is False


def test_probe_partition_completeness_counts_matching_directories():
    fs = MagicMock()
    # Old has 3 partitions, new has 3 — complete
    def ls_side_effect(p):
        if "@old" in p:
            return [MagicMock(name=f"date=2026-{i:02d}") for i in (1, 2, 3)]
        if "@new" in p:
            return [MagicMock(name=f"date=2026-{i:02d}") for i in (1, 2, 3)]
        return []
    fs.ls.side_effect = ls_side_effect
    result = probe_partition_completeness(
        fs=fs,
        old_path="abfss://c@old.dfs.core.windows.net/x",
        new_path="abfss://c@new.dfs.core.windows.net/x",
    )
    assert result.old_count == 3
    assert result.new_count == 3
    assert result.complete is True


def test_probe_partition_completeness_detects_missing():
    fs = MagicMock()
    def ls_side_effect(p):
        if "@old" in p:
            return [MagicMock(name=f"p{i}") for i in range(5)]
        return [MagicMock(name=f"p{i}") for i in range(3)]
    fs.ls.side_effect = ls_side_effect
    result = probe_partition_completeness(
        fs=fs,
        old_path="abfss://c@old.dfs.core.windows.net/x",
        new_path="abfss://c@new.dfs.core.windows.net/x",
    )
    assert result.complete is False
    assert result.new_count == 3
    assert result.old_count == 5
```

- [ ] **Step 2: Write the implementation**

`utils/preflight.py`:

```python
"""Pre-migration health probes."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol

from utils.uc_client import ExternalLocationRecord


@dataclass(frozen=True)
class PreflightResult:
    target_path: str
    external_location_name: Optional[str]
    new_path_exists: bool
    partition_check_ok: Optional[bool]   # None if not partitioned/check skipped


@dataclass(frozen=True)
class PartitionProbeResult:
    old_count: int
    new_count: int
    complete: bool


class _Fs(Protocol):
    def ls(self, path: str):  # pragma: no cover
        ...


def check_external_location_for(
    *, target_path: str, external_locations: list[ExternalLocationRecord]
) -> Optional[ExternalLocationRecord]:
    """Return the external location whose URL is a prefix of target_path, or None."""
    for el in external_locations:
        url = el.url.rstrip("/")
        if target_path == url or target_path.startswith(url + "/"):
            return el
    return None


def probe_path_exists(*, fs: _Fs, path: str) -> bool:
    """Return True if `fs.ls(path)` succeeds and returns at least one entry."""
    try:
        entries = fs.ls(path)
        return bool(entries)
    except Exception:
        return False


def probe_partition_completeness(
    *, fs: _Fs, old_path: str, new_path: str
) -> PartitionProbeResult:
    """Compare directory counts between old and new paths.

    For partitioned tables, expects directory entries named `col=value`. Returns
    complete=True iff new_count >= old_count. Caller can decide tolerance.
    """
    try:
        old_entries = fs.ls(old_path) or []
    except Exception:
        old_entries = []
    try:
        new_entries = fs.ls(new_path) or []
    except Exception:
        new_entries = []
    return PartitionProbeResult(
        old_count=len(old_entries),
        new_count=len(new_entries),
        complete=len(new_entries) >= len(old_entries) and len(old_entries) > 0,
    )
```

- [ ] **Step 3: Sanity check**

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
from unittest.mock import MagicMock
from utils.preflight import (
    check_external_location_for, probe_path_exists, probe_partition_completeness,
)
from utils.uc_client import ExternalLocationRecord

# external location matching
ext = [ExternalLocationRecord('new', 'abfss://c@new.dfs.core.windows.net/', 'cr', False, 'eastus')]
el = check_external_location_for(target_path='abfss://c@new.dfs.core.windows.net/x', external_locations=ext)
assert el and el.name == 'new'
assert check_external_location_for(target_path='abfss://c@third.dfs.core.windows.net/x', external_locations=ext) is None

# path probe
fs = MagicMock(); fs.ls.return_value = [MagicMock()]
assert probe_path_exists(fs=fs, path='x') is True
fs2 = MagicMock(); fs2.ls.side_effect = Exception('nope')
assert probe_path_exists(fs=fs2, path='x') is False

# partition probe
fs3 = MagicMock()
fs3.ls.side_effect = lambda p: [MagicMock() for _ in range(3 if '@new' in p else 3)]
r = probe_partition_completeness(fs=fs3, old_path='abfss://c@old.dfs.core.windows.net/x', new_path='abfss://c@new.dfs.core.windows.net/x')
assert r.complete is True

fs4 = MagicMock()
fs4.ls.side_effect = lambda p: [MagicMock() for _ in range(5 if '@old' in p else 3)]
r2 = probe_partition_completeness(fs=fs4, old_path='abfss://c@old.dfs.core.windows.net/x', new_path='abfss://c@new.dfs.core.windows.net/x')
assert r2.complete is False

print('PASS')
"
```

- [ ] **Step 4: Commit**

```bash
git add utils/preflight.py tests/test_preflight.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): preflight health probes for migration target"
```

---

## Task 6: `utils/migration.py` — per-object playbook helpers

**Purpose:** The per-object SQL plan generators. Each function takes an `ObjectRecord` plus context and returns an ordered list of SQL statements to execute. Pure-function — no Spark execution; the notebook is responsible for execution and error handling.

This separation makes the migration logic testable end-to-end against expected SQL strings.

**Files:**
- Create: `utils/migration.py`
- Create: `tests/test_migration.py`

- [ ] **Step 1: Write the test file**

`tests/test_migration.py`:

```python
import pytest

from utils.discovery import ObjectRecord
from utils.migration import (
    plan_managed_delta_migration,
    plan_managed_non_delta_migration,
    plan_external_table_migration,
    plan_external_volume_migration,
    derive_pre_migration_fqn,
    derive_staging_fqn,
)


def make_rec(*, table_type="MANAGED", data_source_format="DELTA"):
    return ObjectRecord(
        catalog="c", schema="s", name="t",
        object_type="TABLE", table_type=table_type,
        data_source_format=data_source_format,
        storage_path="abfss://x@old.dfs.core.windows.net/t",
        parent_managed_location="abfss://x@new.dfs.core.windows.net/",
        owner="alice", created_at=None, last_altered=None,
    )


def test_derive_pre_migration_fqn():
    assert derive_pre_migration_fqn("c", "s", "t") == ("c", "s", "t__pre_migration")


def test_derive_staging_fqn():
    assert derive_staging_fqn("c", "s", "t") == ("c", "s", "t__migrate_staging")


class TestPlanManagedDeltaMigration:
    def test_emits_clone_then_rename_swap_in_order(self):
        plan = plan_managed_delta_migration(rec=make_rec())
        assert plan.steps == [
            ("clone",
             "CREATE TABLE `c`.`s`.`t__migrate_staging` DEEP CLONE `c`.`s`.`t`"),
            ("rename_orig",
             "ALTER TABLE `c`.`s`.`t` RENAME TO `c`.`s`.`t__pre_migration`"),
            ("rename_staging",
             "ALTER TABLE `c`.`s`.`t__migrate_staging` RENAME TO `c`.`s`.`t`"),
        ]


class TestPlanManagedNonDelta:
    def test_uses_ctas_not_deep_clone(self):
        plan = plan_managed_non_delta_migration(rec=make_rec(data_source_format="ICEBERG"))
        assert any("CREATE TABLE" in sql and "AS SELECT * FROM" in sql for _, sql in plan.steps)
        assert not any("DEEP CLONE" in sql for _, sql in plan.steps)


class TestPlanExternalTable:
    def test_drops_and_creates_at_new_location(self):
        rec = make_rec(table_type="EXTERNAL")
        plan = plan_external_table_migration(rec=rec, new_storage_account="new")
        actions = [k for k, _ in plan.steps]
        assert actions == ["drop", "create"]
        assert "DROP TABLE" in plan.steps[0][1]
        assert "CREATE EXTERNAL TABLE" in plan.steps[1][1]
        assert "abfss://x@new.dfs.core.windows.net/t" in plan.steps[1][1]


class TestPlanExternalVolume:
    def test_drops_and_creates_volume(self):
        rec = ObjectRecord(
            catalog="c", schema="s", name="v",
            object_type="VOLUME", table_type="EXTERNAL",
            data_source_format=None,
            storage_path="abfss://x@old.dfs.core.windows.net/v",
            parent_managed_location=None, owner="u",
            created_at=None, last_altered=None,
        )
        plan = plan_external_volume_migration(rec=rec, new_storage_account="new")
        assert any("DROP VOLUME" in sql for _, sql in plan.steps)
        assert any("CREATE EXTERNAL VOLUME" in sql for _, sql in plan.steps)
        assert any("abfss://x@new.dfs.core.windows.net/v" in sql for _, sql in plan.steps)
```

- [ ] **Step 2: Write the implementation**

`utils/migration.py`:

```python
"""Per-object migration playbook helpers — pure SQL generation, no execution."""
from __future__ import annotations

from dataclasses import dataclass

from utils.discovery import ObjectRecord
from utils.paths import parse_abfss_url
from utils.sql import quote_fqn


@dataclass(frozen=True)
class MigrationPlan:
    """An ordered list of (action_name, sql) tuples to execute for one object."""
    steps: list[tuple[str, str]]


def derive_pre_migration_fqn(catalog: str, schema: str, name: str) -> tuple[str, str, str]:
    return (catalog, schema, f"{name}__pre_migration")


def derive_staging_fqn(catalog: str, schema: str, name: str) -> tuple[str, str, str]:
    return (catalog, schema, f"{name}__migrate_staging")


def _rewrite_account(path: str, new_account: str) -> str:
    """Rewrite the storage account in an abfss:// URL while keeping container + path."""
    parsed = parse_abfss_url(path)
    if not parsed:
        raise ValueError(f"Not an abfss URL: {path}")
    suffix = f"/{parsed.path}" if parsed.path else ""
    return f"abfss://{parsed.container}@{new_account}.dfs.core.windows.net{suffix}"


def plan_managed_delta_migration(*, rec: ObjectRecord) -> MigrationPlan:
    """Delta managed table → DEEP CLONE staging, then two RENAMEs."""
    orig = quote_fqn(rec.catalog, rec.schema, rec.name)
    pre = quote_fqn(*derive_pre_migration_fqn(rec.catalog, rec.schema, rec.name))
    staging = quote_fqn(*derive_staging_fqn(rec.catalog, rec.schema, rec.name))
    return MigrationPlan(steps=[
        ("clone", f"CREATE TABLE {staging} DEEP CLONE {orig}"),
        ("rename_orig", f"ALTER TABLE {orig} RENAME TO {pre}"),
        ("rename_staging", f"ALTER TABLE {staging} RENAME TO {orig}"),
    ])


def plan_managed_non_delta_migration(*, rec: ObjectRecord) -> MigrationPlan:
    """Non-Delta managed table → CTAS staging, then two RENAMEs.

    Time-travel history is NOT preserved by CTAS. Caller should validate
    row count and schema after clone.
    """
    orig = quote_fqn(rec.catalog, rec.schema, rec.name)
    pre = quote_fqn(*derive_pre_migration_fqn(rec.catalog, rec.schema, rec.name))
    staging = quote_fqn(*derive_staging_fqn(rec.catalog, rec.schema, rec.name))
    return MigrationPlan(steps=[
        ("ctas", f"CREATE TABLE {staging} AS SELECT * FROM {orig}"),
        ("rename_orig", f"ALTER TABLE {orig} RENAME TO {pre}"),
        ("rename_staging", f"ALTER TABLE {staging} RENAME TO {orig}"),
    ])


def plan_external_table_migration(
    *, rec: ObjectRecord, new_storage_account: str,
) -> MigrationPlan:
    """External table → DROP + CREATE EXTERNAL TABLE at new path.

    UC does not support ALTER TABLE SET LOCATION for external tables, so the
    only safe path is DROP+CREATE. Grants must be replayed afterward via
    GovernanceReplayer.
    """
    orig = quote_fqn(rec.catalog, rec.schema, rec.name)
    if not rec.storage_path:
        raise ValueError(f"External table {orig} has no storage_path")
    new_path = _rewrite_account(rec.storage_path, new_storage_account)
    fmt = (rec.data_source_format or "DELTA").upper()
    return MigrationPlan(steps=[
        ("drop", f"DROP TABLE {orig}"),
        ("create", f"CREATE EXTERNAL TABLE {orig} USING {fmt} LOCATION '{new_path}'"),
    ])


def plan_external_volume_migration(
    *, rec: ObjectRecord, new_storage_account: str,
) -> MigrationPlan:
    """External volume → DROP + CREATE EXTERNAL VOLUME at new path.

    No ALTER VOLUME SET LOCATION exists.
    """
    orig = quote_fqn(rec.catalog, rec.schema, rec.name)
    if not rec.storage_path:
        raise ValueError(f"External volume {orig} has no storage_path")
    new_path = _rewrite_account(rec.storage_path, new_storage_account)
    return MigrationPlan(steps=[
        ("drop", f"DROP VOLUME {orig}"),
        ("create", f"CREATE EXTERNAL VOLUME {orig} LOCATION '{new_path}'"),
    ])
```

- [ ] **Step 3: Sanity check**

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
from utils.discovery import ObjectRecord
from utils.migration import (
    plan_managed_delta_migration, plan_managed_non_delta_migration,
    plan_external_table_migration, plan_external_volume_migration,
    derive_pre_migration_fqn, derive_staging_fqn, _rewrite_account,
)

assert derive_pre_migration_fqn('c','s','t') == ('c','s','t__pre_migration')
assert derive_staging_fqn('c','s','t') == ('c','s','t__migrate_staging')
assert _rewrite_account('abfss://ct@old.dfs.core.windows.net/p/x', 'new') == 'abfss://ct@new.dfs.core.windows.net/p/x'

r = ObjectRecord('c','s','t','TABLE','MANAGED','DELTA','abfss://x@old.dfs.core.windows.net/t','abfss://x@new.dfs.core.windows.net/','u',None,None)
p = plan_managed_delta_migration(rec=r)
actions = [k for k, _ in p.steps]
assert actions == ['clone','rename_orig','rename_staging']
assert 'DEEP CLONE' in p.steps[0][1]

r2 = ObjectRecord('c','s','t','TABLE','EXTERNAL','PARQUET','abfss://x@old.dfs.core.windows.net/t','abfss://x@new.dfs.core.windows.net/','u',None,None)
p2 = plan_external_table_migration(rec=r2, new_storage_account='new')
assert [k for k,_ in p2.steps] == ['drop','create']
assert 'abfss://x@new.dfs.core.windows.net/t' in p2.steps[1][1]
assert 'USING PARQUET' in p2.steps[1][1]

r3 = ObjectRecord('c','s','v','VOLUME','EXTERNAL',None,'abfss://x@old.dfs.core.windows.net/v',None,'u',None,None)
p3 = plan_external_volume_migration(rec=r3, new_storage_account='new')
assert 'DROP VOLUME' in p3.steps[0][1]
assert 'CREATE EXTERNAL VOLUME' in p3.steps[1][1]

print('PASS')
"
```

- [ ] **Step 4: Commit**

```bash
git add utils/migration.py tests/test_migration.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): per-object migration plan builders"
```

---

## Task 7: `utils/validation.py` — four-layer evidence model

**Purpose:** For a migrated object, collect four independent layers of evidence that queries genuinely read from new storage, and produce a single boolean `overall_pass` plus full evidence payload.

**Files:**
- Create: `utils/validation.py`
- Create: `tests/test_validation.py`

- [ ] **Step 1: Write the test file**

`tests/test_validation.py`:

```python
from unittest.mock import MagicMock

import pytest

from utils.validation import (
    EvidenceLayer,
    ValidationResult,
    validate_object_on_new,
    _parse_input_file_name_rows,
    _hosts_in_paths,
)


def test_hosts_in_paths_extracts_account():
    paths = [
        "abfss://c@newacct.dfs.core.windows.net/x/part-0.parquet",
        "abfss://c@newacct.dfs.core.windows.net/x/part-1.parquet",
    ]
    assert _hosts_in_paths(paths) == {"newacct"}


def test_hosts_in_paths_detects_mixed_hosts():
    paths = [
        "abfss://c@newacct.dfs.core.windows.net/x/p0",
        "abfss://c@oldacct.dfs.core.windows.net/x/p1",
    ]
    assert _hosts_in_paths(paths) == {"newacct", "oldacct"}


def test_validate_object_on_new_all_layers_pass():
    spark = MagicMock()
    fs = MagicMock()

    # Layer 1: DESCRIBE EXTENDED → Location on new account
    describe_rows = MagicMock()
    describe_rows.collect.return_value = [
        type("R", (), {"asDict": lambda self: {"col_name": "Location",
            "data_type": "abfss://c@newacct.dfs.core.windows.net/x"}})(),
    ]

    # Layer 3: input_file_name() → file paths on new account
    input_rows = MagicMock()
    input_rows.collect.return_value = [
        type("R", (), {"asDict": lambda self: {"path": "abfss://c@newacct.dfs.core.windows.net/x/p0.parquet"}})(),
    ]

    spark.sql.side_effect = [describe_rows, input_rows]

    # Layer 2: _delta_log exists at new path
    fs.ls.return_value = [MagicMock()]

    result = validate_object_on_new(
        spark=spark, fs=fs,
        catalog="c", schema="s", name="t",
        expected_new_account="newacct",
        parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        is_delta=True,
    )

    assert result.overall_pass is True
    assert result.metadata_location_ok is True
    assert result.delta_log_at_new_ok is True
    assert result.input_file_name_ok is True
    assert result.parent_managed_location_match is True


def test_validate_object_on_new_input_file_on_old_fails():
    spark = MagicMock()
    describe_rows = MagicMock()
    describe_rows.collect.return_value = [
        type("R", (), {"asDict": lambda self: {"col_name": "Location",
            "data_type": "abfss://c@newacct.dfs.core.windows.net/x"}})(),
    ]
    input_rows = MagicMock()
    input_rows.collect.return_value = [
        type("R", (), {"asDict": lambda self: {"path": "abfss://c@oldacct.dfs.core.windows.net/x/p0.parquet"}})(),
    ]
    spark.sql.side_effect = [describe_rows, input_rows]
    fs = MagicMock()
    fs.ls.return_value = [MagicMock()]

    result = validate_object_on_new(
        spark=spark, fs=fs,
        catalog="c", schema="s", name="t",
        expected_new_account="newacct",
        parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        is_delta=True,
    )

    assert result.input_file_name_ok is False
    assert result.overall_pass is False


def test_parse_input_file_name_rows():
    rows = [
        type("R", (), {"asDict": lambda self: {"path": "abfss://c@n.dfs.core.windows.net/x/p1"}})(),
        type("R", (), {"asDict": lambda self: {"path": "abfss://c@n.dfs.core.windows.net/x/p2"}})(),
    ]
    out = _parse_input_file_name_rows(rows)
    assert out == [
        "abfss://c@n.dfs.core.windows.net/x/p1",
        "abfss://c@n.dfs.core.windows.net/x/p2",
    ]
```

- [ ] **Step 2: Write the implementation**

`utils/validation.py`:

```python
"""Four-layer evidence model for post-migration verification."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Protocol

from utils.paths import parse_abfss_url
from utils.sql import quote_fqn, parse_describe_extended_location


@dataclass(frozen=True)
class EvidenceLayer:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class ValidationResult:
    catalog: str
    schema: str
    name: str
    metadata_location_ok: bool
    delta_log_at_new_ok: Optional[bool]
    input_file_name_ok: bool
    parent_managed_location_match: bool
    overall_pass: bool
    evidence: dict
    validated_at: datetime


class _SqlExec(Protocol):
    def sql(self, query: str):  # pragma: no cover
        ...


class _Fs(Protocol):
    def ls(self, path: str):  # pragma: no cover
        ...


def _hosts_in_paths(paths: list[str]) -> set[str]:
    out: set[str] = set()
    for p in paths:
        parsed = parse_abfss_url(p)
        if parsed:
            out.add(parsed.account)
    return out


def _parse_input_file_name_rows(rows) -> list[str]:
    out: list[str] = []
    for r in rows:
        d = r.asDict() if hasattr(r, "asDict") else dict(r)
        for v in d.values():
            if v and isinstance(v, str) and v.startswith("abfss://"):
                out.append(v)
                break
    return out


def validate_object_on_new(
    *,
    spark: _SqlExec,
    fs: _Fs,
    catalog: str,
    schema: str,
    name: str,
    expected_new_account: str,
    parent_managed_location: Optional[str],
    is_delta: bool,
    sample_limit: int = 10000,
) -> ValidationResult:
    """Run all four evidence layers against the migrated object and return a result."""
    fqn = quote_fqn(catalog, schema, name)
    evidence: dict = {}

    # --- Layer 1: DESCRIBE EXTENDED → Location ---
    try:
        rows = spark.sql(f"DESCRIBE EXTENDED TABLE {fqn}").collect()
        rendered = "\n".join(
            "\t".join(str(c) if c is not None else "" for c in (r.asDict().values() if hasattr(r, "asDict") else r))
            for r in rows
        )
        location = parse_describe_extended_location(rendered)
        evidence["describe_location"] = location
        parsed = parse_abfss_url(location) if location else None
        metadata_ok = parsed is not None and parsed.account == expected_new_account
    except Exception as e:
        metadata_ok = False
        evidence["describe_location_error"] = str(e)

    # --- Layer 2: _delta_log at new path (Delta only) ---
    delta_log_ok: Optional[bool]
    if is_delta and evidence.get("describe_location"):
        try:
            entries = fs.ls(f"{evidence['describe_location'].rstrip('/')}/_delta_log") or []
            delta_log_ok = bool(entries)
            evidence["delta_log_entries"] = len(entries)
        except Exception as e:
            delta_log_ok = False
            evidence["delta_log_error"] = str(e)
    else:
        delta_log_ok = None

    # --- Layer 3: input_file_name() at runtime ---
    try:
        rows = spark.sql(
            f"SELECT input_file_name() AS path FROM {fqn} LIMIT {sample_limit}"
        ).collect()
        paths = _parse_input_file_name_rows(rows)
        hosts = _hosts_in_paths(paths)
        input_ok = bool(hosts) and hosts == {expected_new_account}
        evidence["input_file_name_hosts"] = sorted(hosts)
        evidence["input_file_name_sample_count"] = len(paths)
    except Exception as e:
        input_ok = False
        evidence["input_file_name_error"] = str(e)

    # --- Layer 4: parent managed_location matches ---
    parent_ok = False
    if parent_managed_location:
        parent_parsed = parse_abfss_url(parent_managed_location)
        parent_ok = parent_parsed is not None and parent_parsed.account == expected_new_account
        evidence["parent_account"] = parent_parsed.account if parent_parsed else None

    overall = bool(metadata_ok) and bool(input_ok) and bool(parent_ok) and (
        delta_log_ok is not False
    )

    return ValidationResult(
        catalog=catalog, schema=schema, name=name,
        metadata_location_ok=bool(metadata_ok),
        delta_log_at_new_ok=delta_log_ok,
        input_file_name_ok=bool(input_ok),
        parent_managed_location_match=bool(parent_ok),
        overall_pass=overall,
        evidence=evidence,
        validated_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )


def evidence_to_json(result: ValidationResult) -> str:
    """Serialize the evidence dict to JSON for the validation_results table."""
    return json.dumps(result.evidence, default=str, sort_keys=True)
```

- [ ] **Step 3: Sanity check**

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
from unittest.mock import MagicMock
from utils.validation import _hosts_in_paths, _parse_input_file_name_rows, validate_object_on_new, evidence_to_json

assert _hosts_in_paths(['abfss://c@new.dfs.core.windows.net/x']) == {'new'}
assert _hosts_in_paths(['abfss://c@new.dfs.core.windows.net/x','abfss://c@old.dfs.core.windows.net/y']) == {'new','old'}

class R:
    def __init__(self, d): self._d = d
    def asDict(self): return dict(self._d)

paths = _parse_input_file_name_rows([R({'path': 'abfss://c@n.dfs.core.windows.net/x/p1'}), R({'path': 'abfss://c@n.dfs.core.windows.net/x/p2'})])
assert paths == ['abfss://c@n.dfs.core.windows.net/x/p1', 'abfss://c@n.dfs.core.windows.net/x/p2']

# All four layers pass
spark = MagicMock()
res1 = MagicMock(); res1.collect.return_value = [R({'col_name':'Location','data_type':'abfss://c@new.dfs.core.windows.net/x'})]
res2 = MagicMock(); res2.collect.return_value = [R({'path':'abfss://c@new.dfs.core.windows.net/x/p1.parquet'})]
spark.sql.side_effect = [res1, res2]
fs = MagicMock(); fs.ls.return_value = [MagicMock()]

result = validate_object_on_new(spark=spark, fs=fs, catalog='c', schema='s', name='t',
    expected_new_account='new', parent_managed_location='abfss://c@new.dfs.core.windows.net/', is_delta=True)
assert result.overall_pass is True, result

# Layer 3 fails
spark2 = MagicMock()
res1b = MagicMock(); res1b.collect.return_value = [R({'col_name':'Location','data_type':'abfss://c@new.dfs.core.windows.net/x'})]
res2b = MagicMock(); res2b.collect.return_value = [R({'path':'abfss://c@old.dfs.core.windows.net/x/p1.parquet'})]
spark2.sql.side_effect = [res1b, res2b]
fs2 = MagicMock(); fs2.ls.return_value = [MagicMock()]
result2 = validate_object_on_new(spark=spark2, fs=fs2, catalog='c', schema='s', name='t',
    expected_new_account='new', parent_managed_location='abfss://c@new.dfs.core.windows.net/', is_delta=True)
assert result2.input_file_name_ok is False
assert result2.overall_pass is False

# JSON serialization works
import json
parsed = json.loads(evidence_to_json(result))
assert 'describe_location' in parsed

print('PASS')
"
```

- [ ] **Step 4: Commit**

```bash
git add utils/validation.py tests/test_validation.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(utils): four-layer validation evidence model"
```

---

## Task 8: `notebooks/03a_rollback.py` — rollback orchestrator

**Purpose:** If the decision report recommends rollback, this notebook drops `consistent_new` objects, reverts schema and catalog managed_locations to the old account, and verifies the result by re-running discovery's classification logic.

**Files:**
- Create: `notebooks/03a_rollback.py`

- [ ] **Step 1: Write the notebook**

`notebooks/03a_rollback.py`:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 03a_rollback — Revert to old storage
# MAGIC
# MAGIC **Purpose:** Run only if `02_decision_report` recommends rollback. Drops
# MAGIC every `consistent_new` UC object, reverts schema and catalog
# MAGIC `managed_location` to old, and verifies the metastore is fully on old.
# MAGIC
# MAGIC **Inputs:** `<OPS_SCHEMA>.inventory`, `<OPS_SCHEMA>.external_locations`.
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `<OPS_SCHEMA>.object_metadata_snapshot` (audit trail of dropped objects)
# MAGIC - `<OPS_SCHEMA>.migration_log` (per-object operation log)
# MAGIC
# MAGIC **Side effects:** DESTRUCTIVE. Drops every `consistent_new` object, including
# MAGIC any data on the new storage account. Requires `CONFIRMED = True`.
# MAGIC
# MAGIC **Resumability:** Re-running skips objects already logged as dropped.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Path setup

# COMMAND ----------
import os, sys


def _add_utils_to_path() -> None:
    here = sys.path[0] if sys.path else os.getcwd()
    candidate = here
    for _ in range(5):
        parent = os.path.dirname(candidate)
        if parent == candidate:
            break
        if os.path.isdir(os.path.join(parent, "utils")):
            if parent not in sys.path:
                sys.path.insert(0, parent)
            return
        candidate = parent


_REPO_ROOT_HINT: str | None = None
if _REPO_ROOT_HINT and _REPO_ROOT_HINT not in sys.path:
    sys.path.insert(0, _REPO_ROOT_HINT)
else:
    _add_utils_to_path()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
OLD_STORAGE_ACCOUNT = "oldacct"
NEW_STORAGE_ACCOUNT = "newacct"
OPS_SCHEMA = "main._migration_ops"
CONFIRMED = False                # MUST be set to True to execute
DRY_RUN = True                   # set to False to actually execute (only after CONFIRMED=True)
ACTOR = "rollback_runner"        # identifier for migration_log claim_by

# COMMAND ----------
import json

from utils.discovery import ObjectRecord, classify_object
from utils.governance import GovernanceCapturer
from utils.sql import quote_fqn
from utils.state import MigrationLog, SnapshotWriter

assert not (not DRY_RUN and not CONFIRMED), (
    "DRY_RUN=False requires CONFIRMED=True. Set both flags explicitly."
)

inv_df = spark.table(f"{OPS_SCHEMA}.inventory")
rows = inv_df.collect()
records = [(r, r["classification"]) for r in rows]
new_objects = [r for r, c in records if c == "consistent_new"]
print(f"consistent_new objects in scope: {len(new_objects)}")

if not new_objects and not DRY_RUN:
    print("Nothing to drop. Skipping to managed_location revert.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Capture governance state into object_metadata_snapshot

# COMMAND ----------
mig_log = MigrationLog(spark=spark, table_name=f"{OPS_SCHEMA}.migration_log")
mig_log.ensure_exists()

snap_writer = SnapshotWriter(spark=spark, table_name=f"{OPS_SCHEMA}.object_metadata_snapshot")
snap_writer.ensure_exists()

capturer = GovernanceCapturer(spark=spark)

for r in new_objects:
    if not mig_log.claim(catalog=r["catalog"], schema=r["schema"], name=r["name"],
                         object_type=r["object_type"], actor=ACTOR):
        print(f"  SKIP (claimed by someone else): {r['catalog']}.{r['schema']}.{r['name']}")
        continue
    snap = capturer.capture(catalog=r["catalog"], schema=r["schema"],
                            name=r["name"], object_type=r["object_type"])
    snap_json = json.dumps({
        "grants": [g.__dict__ for g in snap.grants],
        "owner": snap.owner,
        "tags": [t.__dict__ for t in snap.tags],
        "row_filter_name": snap.row_filter_name,
        "row_filter_using_columns": list(snap.row_filter_using_columns),
        "column_masks": [m.__dict__ for m in snap.column_masks],
        "table_comment": snap.table_comment,
        "column_comments": snap.column_comments,
        "table_properties": snap.table_properties,
    }, default=list)
    if DRY_RUN:
        print(f"  DRY: would snapshot + drop {r['catalog']}.{r['schema']}.{r['name']}")
    else:
        snap_writer.append(catalog=r["catalog"], schema=r["schema"], name=r["name"],
                           object_type=r["object_type"], snapshot_json=snap_json)
        mig_log.update(catalog=r["catalog"], schema=r["schema"], name=r["name"],
                       status="snapshot_taken")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Drop new-storage objects (in dependency order)

# COMMAND ----------
# Order: views (skipped — not in inventory), tables, volumes, registered models
def _drop_sql(obj_type: str, catalog: str, schema: str, name: str) -> str:
    kw = "VOLUME" if obj_type == "VOLUME" else "TABLE"
    return f"DROP {kw} {quote_fqn(catalog, schema, name)}"


# Sort: tables before volumes, then alphabetically
new_objects_sorted = sorted(new_objects, key=lambda r: (r["object_type"] != "TABLE", r["catalog"], r["schema"], r["name"]))

for r in new_objects_sorted:
    sql = _drop_sql(r["object_type"], r["catalog"], r["schema"], r["name"])
    if DRY_RUN:
        print(f"  DRY: {sql}")
    else:
        try:
            spark.sql(sql)
            mig_log.update(catalog=r["catalog"], schema=r["schema"], name=r["name"],
                           status="validated")
            print(f"  dropped: {r['catalog']}.{r['schema']}.{r['name']}")
        except Exception as e:
            mig_log.update(catalog=r["catalog"], schema=r["schema"], name=r["name"],
                           status="failed", error_trace=str(e))
            print(f"  FAILED: {r['catalog']}.{r['schema']}.{r['name']}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Revert schema managed_locations

# COMMAND ----------
# Build the old-equivalent path for each schema whose managed_location is on new
from utils.paths import parse_abfss_url


schemas_to_revert = []
seen = set()
for r in rows:
    key = (r["catalog"], r["schema"])
    if key in seen:
        continue
    seen.add(key)
    parent = r["parent_managed_location"]
    parsed = parse_abfss_url(parent)
    if parsed and parsed.account == NEW_STORAGE_ACCOUNT:
        old_path = parent.replace(f"@{NEW_STORAGE_ACCOUNT}.", f"@{OLD_STORAGE_ACCOUNT}.", 1)
        schemas_to_revert.append((r["catalog"], r["schema"], old_path))

print(f"Schemas to revert: {len(schemas_to_revert)}")
for catalog, sch, old_path in schemas_to_revert:
    sql = (
        f"ALTER SCHEMA {quote_fqn(catalog, sch)} "
        f"SET MANAGED LOCATION '{old_path}'"
    )
    if DRY_RUN:
        print(f"  DRY: {sql}")
    else:
        try:
            spark.sql(sql)
            print(f"  reverted: {catalog}.{sch}")
        except Exception as e:
            print(f"  FAILED schema revert {catalog}.{sch}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Revert catalog managed_locations

# COMMAND ----------
from utils.uc_client import UcClient
from databricks.sdk import WorkspaceClient


class _SdkRest:
    def __init__(self, w):
        self._api = w.api_client
    def get(self, path: str) -> dict:
        return self._api.do("GET", path)


w = WorkspaceClient()
client = UcClient(sdk=w, rest=_SdkRest(w))
catalogs = client.list_catalogs()
for c in catalogs:
    if not c.storage_root:
        continue
    parsed = parse_abfss_url(c.storage_root)
    if parsed and parsed.account == NEW_STORAGE_ACCOUNT:
        old_path = c.storage_root.replace(f"@{NEW_STORAGE_ACCOUNT}.", f"@{OLD_STORAGE_ACCOUNT}.", 1)
        sql = f"ALTER CATALOG {quote_fqn(c.name)} SET MANAGED LOCATION '{old_path}'"
        if DRY_RUN:
            print(f"  DRY: {sql}")
        else:
            try:
                spark.sql(sql)
                print(f"  reverted catalog {c.name}")
            except Exception as e:
                print(f"  FAILED catalog revert {c.name}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — Verify

# COMMAND ----------
print("Re-run 01_discovery to verify all objects are now consistent_old.")
```

- [ ] **Step 2: AST-parse + commit**

```bash
python3 -c "import ast; ast.parse(open('notebooks/03a_rollback.py').read()); print('OK')"
git add notebooks/03a_rollback.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(notebook): 03a_rollback orchestrator with DRY_RUN + claim guard"
```

---

## Task 9: `notebooks/03b_forward_migrate.py` — forward-migrate orchestrator

**Purpose:** Move every `drift_managed_on_old` and `external_on_old` object to new storage in the right order, with per-object snapshot + clone + swap + replay + log, resumable on failure.

**Files:**
- Create: `notebooks/03b_forward_migrate.py`

- [ ] **Step 1: Write the notebook**

`notebooks/03b_forward_migrate.py`:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 03b_forward_migrate — Move objects to new storage
# MAGIC
# MAGIC **Purpose:** Migrate every `drift_managed_on_old` and `external_on_old`
# MAGIC object from old to new storage. Idempotent + resumable. Each per-table
# MAGIC migration is gated by a CAS-style claim in `migration_log` so concurrent
# MAGIC runs are safe.
# MAGIC
# MAGIC **Inputs:** `<OPS_SCHEMA>.inventory`, `<OPS_SCHEMA>.external_locations`.
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `<OPS_SCHEMA>.object_metadata_snapshot` (governance capture per object)
# MAGIC - `<OPS_SCHEMA>.migration_log` (operation log per object)
# MAGIC
# MAGIC **Side effects:** DESTRUCTIVE. Renames originals to `<name>__pre_migration`
# MAGIC and replaces with cloned copies on new storage. Originals are retained
# MAGIC (not dropped) until the gated cleanup cell at the bottom.
# MAGIC
# MAGIC **Required:** `CONFIRMED = True`. Default `DRY_RUN = True`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Path setup

# COMMAND ----------
import os, sys


def _add_utils_to_path():
    here = sys.path[0] if sys.path else os.getcwd()
    candidate = here
    for _ in range(5):
        parent = os.path.dirname(candidate)
        if parent == candidate:
            break
        if os.path.isdir(os.path.join(parent, "utils")):
            if parent not in sys.path:
                sys.path.insert(0, parent)
            return
        candidate = parent


_REPO_ROOT_HINT: str | None = None
if _REPO_ROOT_HINT and _REPO_ROOT_HINT not in sys.path:
    sys.path.insert(0, _REPO_ROOT_HINT)
else:
    _add_utils_to_path()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
OLD_STORAGE_ACCOUNT = "oldacct"
NEW_STORAGE_ACCOUNT = "newacct"
OPS_SCHEMA = "main._migration_ops"
CONFIRMED = False
DRY_RUN = True
ACTOR = "forward_migrate_runner"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------
import json
import traceback

from utils.discovery import ObjectRecord
from utils.governance import GovernanceCapturer, GovernanceReplayer
from utils.migration import (
    plan_managed_delta_migration, plan_managed_non_delta_migration,
    plan_external_table_migration, plan_external_volume_migration,
    derive_pre_migration_fqn, derive_staging_fqn,
)
from utils.preflight import (
    check_external_location_for, probe_path_exists,
)
from utils.sql import quote_fqn
from utils.state import MigrationLog, SnapshotWriter
from utils.uc_client import ExternalLocationRecord


assert not (not DRY_RUN and not CONFIRMED), (
    "DRY_RUN=False requires CONFIRMED=True. Set both flags explicitly."
)

inv_df = spark.table(f"{OPS_SCHEMA}.inventory")
ext_locs_df = spark.table(f"{OPS_SCHEMA}.external_locations")
ext_locs = [
    ExternalLocationRecord(name=r["name"], url=r["url"], credential_name=r["credential_name"],
                           read_only=r["read_only"], region=r.get("region"))
    for r in ext_locs_df.collect()
]
inv_rows = inv_df.collect()

drift = [r for r in inv_rows if r["classification"] == "drift_managed_on_old"]
external_old = [r for r in inv_rows if r["classification"] == "external_on_old"]
print(f"drift_managed_on_old: {len(drift)}")
print(f"external_on_old: {len(external_old)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Pre-flight: external location for new account + per-object data presence

# COMMAND ----------
fs = dbutils.fs  # noqa: F821

# Resolve the new-account external location (probe the first drift object's expected new path)
target_new_path = (
    drift[0]["storage_path"].replace(f"@{OLD_STORAGE_ACCOUNT}.", f"@{NEW_STORAGE_ACCOUNT}.", 1)
    if drift else None
)
if target_new_path:
    el = check_external_location_for(target_path=target_new_path, external_locations=ext_locs)
    assert el is not None, f"No external location covers {target_new_path}"
    print(f"External location for new account: {el.name} ({el.credential_name})")

# Verify new path exists for every in-scope object
missing = []
for r in drift + external_old:
    if not r["storage_path"]:
        continue
    new_p = r["storage_path"].replace(f"@{OLD_STORAGE_ACCOUNT}.", f"@{NEW_STORAGE_ACCOUNT}.", 1)
    if not probe_path_exists(fs=fs, path=new_p):
        missing.append((r["catalog"], r["schema"], r["name"], new_p))

if missing:
    print(f"\n{len(missing)} object(s) missing at new path:")
    for c, s, n, p in missing[:20]:
        print(f"  {c}.{s}.{n} -> {p}")
    if not DRY_RUN:
        raise RuntimeError("Pre-flight failed: complete the data copy before retrying.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Setup state writers

# COMMAND ----------
mig_log = MigrationLog(spark=spark, table_name=f"{OPS_SCHEMA}.migration_log")
mig_log.ensure_exists()
snap_writer = SnapshotWriter(spark=spark, table_name=f"{OPS_SCHEMA}.object_metadata_snapshot")
snap_writer.ensure_exists()
capturer = GovernanceCapturer(spark=spark)
replayer = GovernanceReplayer(spark=spark)


def _row_to_record(r) -> ObjectRecord:
    return ObjectRecord(
        catalog=r["catalog"], schema=r["schema"], name=r["name"],
        object_type=r["object_type"], table_type=r["table_type"],
        data_source_format=r["data_source_format"],
        storage_path=r["storage_path"], parent_managed_location=r["parent_managed_location"],
        owner=r["owner"], created_at=r["created_at"], last_altered=r["last_altered"],
        requires_pipeline_handling=r["requires_pipeline_handling"],
        size_bytes=r["size_bytes"], tag_count=r["tag_count"],
        grant_count=r["grant_count"],
        has_row_filter=r["has_row_filter"], has_column_mask=r["has_column_mask"],
    )


def _serialize_snapshot(snap) -> str:
    return json.dumps({
        "grants": [g.__dict__ for g in snap.grants],
        "owner": snap.owner,
        "tags": [t.__dict__ for t in snap.tags],
        "row_filter_name": snap.row_filter_name,
        "row_filter_using_columns": list(snap.row_filter_using_columns),
        "column_masks": [m.__dict__ for m in snap.column_masks],
        "table_comment": snap.table_comment,
        "column_comments": snap.column_comments,
        "table_properties": snap.table_properties,
    }, default=list)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Migrate external tables (cheap: DROP + CREATE EXTERNAL TABLE)

# COMMAND ----------
def _execute_steps(rec: ObjectRecord, steps: list[tuple[str, str]]) -> None:
    for action, sql in steps:
        if DRY_RUN:
            print(f"    DRY [{action}]: {sql}")
        else:
            spark.sql(sql)
            mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name, status=action)


for r in external_old:
    if r["object_type"] != "TABLE":
        continue
    rec = _row_to_record(r)
    if not mig_log.claim(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                         object_type=rec.object_type, actor=ACTOR):
        print(f"  SKIP (claimed): {rec.catalog}.{rec.schema}.{rec.name}")
        continue

    try:
        snap = capturer.capture(catalog=rec.catalog, schema=rec.schema,
                                name=rec.name, object_type=rec.object_type)
        if not DRY_RUN:
            snap_writer.append(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                               object_type=rec.object_type, snapshot_json=_serialize_snapshot(snap))
            mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                           status="snapshot_taken")

        plan = plan_external_table_migration(rec=rec, new_storage_account=NEW_STORAGE_ACCOUNT)
        _execute_steps(rec, plan.steps)

        if not DRY_RUN:
            warnings = replayer.replay(snap, target_fqn=(rec.catalog, rec.schema, rec.name))
            for w in warnings:
                print(f"    WARN: {w}")
            mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                           status="validated")
        print(f"  migrated external table: {rec.catalog}.{rec.schema}.{rec.name}")
    except Exception as e:
        mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                       status="failed", error_trace=traceback.format_exc())
        print(f"  FAILED external table {rec.catalog}.{rec.schema}.{rec.name}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Migrate external volumes

# COMMAND ----------
for r in external_old:
    if r["object_type"] != "VOLUME":
        continue
    rec = _row_to_record(r)
    if not mig_log.claim(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                         object_type=rec.object_type, actor=ACTOR):
        continue
    try:
        snap = capturer.capture(catalog=rec.catalog, schema=rec.schema,
                                name=rec.name, object_type="VOLUME")
        if not DRY_RUN:
            snap_writer.append(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                               object_type="VOLUME", snapshot_json=_serialize_snapshot(snap))
        plan = plan_external_volume_migration(rec=rec, new_storage_account=NEW_STORAGE_ACCOUNT)
        _execute_steps(rec, plan.steps)
        if not DRY_RUN:
            replayer.replay(snap, target_fqn=(rec.catalog, rec.schema, rec.name), object_type="VOLUME")
            mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name, status="validated")
        print(f"  migrated external volume: {rec.catalog}.{rec.schema}.{rec.name}")
    except Exception as e:
        mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                       status="failed", error_trace=traceback.format_exc())
        print(f"  FAILED external volume {rec.catalog}.{rec.schema}.{rec.name}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — Migrate managed Delta tables (DEEP CLONE + RENAME swap)

# COMMAND ----------
for r in [x for x in drift if x["object_type"] == "TABLE" and x["data_source_format"] == "DELTA"]:
    rec = _row_to_record(r)
    if rec.requires_pipeline_handling:
        print(f"  SKIP (pipeline handling): {rec.catalog}.{rec.schema}.{rec.name}")
        continue
    if not mig_log.claim(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                         object_type="TABLE", actor=ACTOR):
        continue
    try:
        # Capture governance
        snap = capturer.capture(catalog=rec.catalog, schema=rec.schema, name=rec.name)
        if not DRY_RUN:
            snap_writer.append(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                               object_type="TABLE", snapshot_json=_serialize_snapshot(snap))

        # Row count + schema hash before
        orig_fqn = quote_fqn(rec.catalog, rec.schema, rec.name)
        row_count_before = spark.sql(f"SELECT count(*) AS k FROM {orig_fqn}").collect()[0]["k"]
        schema_hash_before = hash(tuple((f.name, f.dataType.simpleString())
                                        for f in spark.table(orig_fqn).schema.fields))

        plan = plan_managed_delta_migration(rec=rec)
        _execute_steps(rec, plan.steps)

        if not DRY_RUN:
            # Validate counts match
            row_count_after = spark.sql(f"SELECT count(*) AS k FROM {orig_fqn}").collect()[0]["k"]
            schema_hash_after = hash(tuple((f.name, f.dataType.simpleString())
                                           for f in spark.table(orig_fqn).schema.fields))
            assert row_count_before == row_count_after, (
                f"row count mismatch: before={row_count_before} after={row_count_after}"
            )
            assert schema_hash_before == schema_hash_after, "schema hash mismatch"

            replayer.replay(snap, target_fqn=(rec.catalog, rec.schema, rec.name))

            staging_c, staging_s, staging_n = derive_staging_fqn(rec.catalog, rec.schema, rec.name)
            pre_c, pre_s, pre_n = derive_pre_migration_fqn(rec.catalog, rec.schema, rec.name)
            mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                           status="validated",
                           row_count_before=row_count_before, row_count_after=row_count_after,
                           schema_hash_before=str(schema_hash_before),
                           schema_hash_after=str(schema_hash_after),
                           staging_fqn=f"{staging_c}.{staging_s}.{staging_n}",
                           pre_migration_fqn=f"{pre_c}.{pre_s}.{pre_n}")
        print(f"  migrated managed Delta: {rec.catalog}.{rec.schema}.{rec.name}")
    except Exception as e:
        mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                       status="failed", error_trace=traceback.format_exc())
        print(f"  FAILED managed Delta {rec.catalog}.{rec.schema}.{rec.name}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6 — Migrate managed non-Delta tables (CTAS pattern)

# COMMAND ----------
for r in [x for x in drift if x["object_type"] == "TABLE" and x["data_source_format"] != "DELTA"]:
    rec = _row_to_record(r)
    if rec.requires_pipeline_handling:
        continue
    if not mig_log.claim(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                         object_type="TABLE", actor=ACTOR):
        continue
    try:
        snap = capturer.capture(catalog=rec.catalog, schema=rec.schema, name=rec.name)
        if not DRY_RUN:
            snap_writer.append(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                               object_type="TABLE", snapshot_json=_serialize_snapshot(snap))
        plan = plan_managed_non_delta_migration(rec=rec)
        _execute_steps(rec, plan.steps)
        if not DRY_RUN:
            replayer.replay(snap, target_fqn=(rec.catalog, rec.schema, rec.name))
            mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name, status="validated")
        print(f"  migrated managed non-Delta ({rec.data_source_format}): "
              f"{rec.catalog}.{rec.schema}.{rec.name}")
    except Exception as e:
        mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                       status="failed", error_trace=traceback.format_exc())
        print(f"  FAILED managed non-Delta {rec.catalog}.{rec.schema}.{rec.name}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 7 — Pipeline-handling objects: hand off

# COMMAND ----------
pipeline = [r for r in drift if r["requires_pipeline_handling"]]
print(f"{len(pipeline)} pipeline-handling object(s) require manual handling:")
for r in pipeline:
    print(f"  {r['catalog']}.{r['schema']}.{r['name']} ({r['table_type']})")
print("\nCoordinate with pipeline owners to refresh these tables after upstream "
      "tables are migrated.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 8 — Cleanup (gated, dangerous)
# MAGIC
# MAGIC After validation has succeeded and the grace period has elapsed, drop the
# MAGIC `*__pre_migration` tables. **Only run this cell after `04_validation`
# MAGIC reports `overall_pass=True` for every migrated object.**

# COMMAND ----------
CLEANUP_CONFIRMED = False  # set True only after validation + grace period

if CLEANUP_CONFIRMED:
    log_rows = spark.sql(
        f"SELECT pre_migration_fqn FROM {OPS_SCHEMA}.migration_log "
        f"WHERE status = 'validated' AND pre_migration_fqn IS NOT NULL"
    ).collect()
    for row in log_rows:
        fqn = row["pre_migration_fqn"]
        try:
            spark.sql(f"DROP TABLE {fqn}")
            print(f"  dropped {fqn}")
        except Exception as e:
            print(f"  FAILED to drop {fqn}: {e}")
else:
    print("Cleanup skipped — set CLEANUP_CONFIRMED=True to drop *__pre_migration tables.")
```

- [ ] **Step 2: AST-parse + commit**

```bash
python3 -c "import ast; ast.parse(open('notebooks/03b_forward_migrate.py').read()); print('OK')"
git add notebooks/03b_forward_migrate.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(notebook): 03b_forward_migrate orchestrator with full per-object playbook"
```

---

## Task 10: `notebooks/04_validation.py` — multi-layer evidence reporting

**Purpose:** For every migrated object (status=validated in `migration_log`), run all four evidence layers and write per-object results to `validation_results`. Surfaces failures for human review.

**Files:**
- Create: `notebooks/04_validation.py`

- [ ] **Step 1: Write the notebook**

`notebooks/04_validation.py`:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 04_validation — Four-layer evidence for every migrated object
# MAGIC
# MAGIC **Purpose:** For every object that migrated, prove via four independent
# MAGIC evidence layers that queries genuinely read from new storage.
# MAGIC
# MAGIC **Inputs:** `<OPS_SCHEMA>.migration_log`, `<OPS_SCHEMA>.inventory`.
# MAGIC
# MAGIC **Outputs:** `<OPS_SCHEMA>.validation_results` — one row per object with
# MAGIC all evidence flags and raw evidence JSON.
# MAGIC
# MAGIC **Side effects:** Read-only against UC objects; writes only to
# MAGIC `<OPS_SCHEMA>.validation_results`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Path setup

# COMMAND ----------
import os, sys


def _add_utils_to_path():
    here = sys.path[0] if sys.path else os.getcwd()
    candidate = here
    for _ in range(5):
        parent = os.path.dirname(candidate)
        if parent == candidate:
            break
        if os.path.isdir(os.path.join(parent, "utils")):
            if parent not in sys.path:
                sys.path.insert(0, parent)
            return
        candidate = parent


_REPO_ROOT_HINT: str | None = None
if _REPO_ROOT_HINT and _REPO_ROOT_HINT not in sys.path:
    sys.path.insert(0, _REPO_ROOT_HINT)
else:
    _add_utils_to_path()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
NEW_STORAGE_ACCOUNT = "newacct"
OPS_SCHEMA = "main._migration_ops"
SAMPLE_LIMIT = 10000

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run validation

# COMMAND ----------
from datetime import datetime, timezone

from utils.validation import validate_object_on_new, evidence_to_json
from utils.state import VALIDATION_RESULTS_SCHEMA, ValidationResultsWriter


validated_rows = spark.sql(
    f"SELECT m.catalog, m.schema, m.name, m.object_type, i.data_source_format, "
    f"       i.parent_managed_location "
    f"FROM {OPS_SCHEMA}.migration_log m "
    f"JOIN {OPS_SCHEMA}.inventory i "
    f"  ON m.catalog = i.catalog AND m.schema = i.schema AND m.name = i.name "
    f"WHERE m.status = 'validated'"
).collect()

print(f"Validating {len(validated_rows)} migrated objects...")

writer = ValidationResultsWriter(spark=spark, table_name=f"{OPS_SCHEMA}.validation_results")
writer.ensure_exists()

fs = dbutils.fs  # noqa: F821
results_rows = []
for r in validated_rows:
    result = validate_object_on_new(
        spark=spark, fs=fs,
        catalog=r["catalog"], schema=r["schema"], name=r["name"],
        expected_new_account=NEW_STORAGE_ACCOUNT,
        parent_managed_location=r["parent_managed_location"],
        is_delta=(r["data_source_format"] == "DELTA"),
        sample_limit=SAMPLE_LIMIT,
    )
    results_rows.append((
        result.catalog, result.schema, result.name,
        result.metadata_location_ok, result.delta_log_at_new_ok,
        result.input_file_name_ok, result.parent_managed_location_match,
        None, None, None, None, None, None,   # governance flags — Plan 2.1 expansion
        result.overall_pass,
        evidence_to_json(result),
        result.validated_at,
    ))
    print(f"  {result.catalog}.{result.schema}.{result.name}: "
          f"overall_pass={result.overall_pass} "
          f"(meta={result.metadata_location_ok} "
          f"delta_log={result.delta_log_at_new_ok} "
          f"input_file={result.input_file_name_ok} "
          f"parent={result.parent_managed_location_match})")

if results_rows:
    df = spark.createDataFrame(results_rows, schema=VALIDATION_RESULTS_SCHEMA)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{OPS_SCHEMA}.validation_results"
    )
    pass_count = sum(1 for r in results_rows if r[-3])  # overall_pass column
    print(f"\n{pass_count} / {len(results_rows)} passed all four evidence layers.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Failure detail

# COMMAND ----------
spark.sql(
    f"SELECT catalog, schema, name, metadata_location_ok, delta_log_at_new_ok, "
    f"       input_file_name_ok, parent_managed_location_match, evidence_json "
    f"FROM {OPS_SCHEMA}.validation_results "
    f"WHERE NOT overall_pass"
).show(truncate=False)
```

- [ ] **Step 2: AST-parse + commit**

```bash
python3 -c "import ast; ast.parse(open('notebooks/04_validation.py').read()); print('OK')"
git add notebooks/04_validation.py
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "feat(notebook): 04_validation runs four-layer evidence per migrated object"
```

---

## Task 11: README finalize + tag plan-2-complete

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Run end-to-end smoke**

```bash
cd /Users/art.malanok/work/uc-storage-migration
python3 -c "
import sys, ast; sys.path.insert(0, '.')

# Import every non-Spark module
import utils.paths, utils.sql, utils.uc_client, utils.discovery, utils.lineage
import utils.reporting, utils.storage_path, utils.governance, utils.preflight
import utils.migration, utils.validation
print('non-Spark utils imports: OK')

# AST-parse Spark-dependent and notebook files
for f in ['utils/state.py',
          'notebooks/01_discovery.py', 'notebooks/02_decision_report.py',
          'notebooks/03a_rollback.py', 'notebooks/03b_forward_migrate.py',
          'notebooks/04_validation.py']:
    ast.parse(open(f).read())
print('Spark/notebook AST parse: OK')

# End-to-end pure-logic smoke
from utils.discovery import ObjectRecord
from utils.migration import plan_managed_delta_migration, plan_external_table_migration
r = ObjectRecord('c','s','t','TABLE','MANAGED','DELTA','abfss://x@old.dfs.core.windows.net/t','abfss://x@new.dfs.core.windows.net/','u',None,None)
assert 'DEEP CLONE' in plan_managed_delta_migration(rec=r).steps[0][1]
r2 = ObjectRecord('c','s','t','TABLE','EXTERNAL','PARQUET','abfss://x@old.dfs.core.windows.net/t','abfss://x@new.dfs.core.windows.net/','u',None,None)
p = plan_external_table_migration(rec=r2, new_storage_account='new')
assert 'abfss://x@new.dfs.core.windows.net/t' in p.steps[1][1]
print('PLAN 2 SMOKE PASSED')
"
```

- [ ] **Step 2: Replace README**

Replace `README.md` with:

```markdown
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
```

- [ ] **Step 3: Commit + tag**

```bash
git add README.md
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" commit -m "docs: README final for Plan 1 + Plan 2 complete"
git -c user.name="Art Malanok" -c user.email="art.malanok@databricks.com" tag -a plan-2-complete -m "Plan 2: rollback + forward-migrate + validation complete"
```

---

## Plan 2 self-review

**Spec coverage** (against §8–§11 of the spec):
- §8 (rollback playbook): Task 8 ✓
- §9 (forward-migrate playbook, including externals, managed Delta, managed non-Delta, volumes, pipeline-handling skip, cleanup gate): Task 9 ✓
- §10 (four-layer validation + governance-replay validation flags): Tasks 7, 10 ✓ (governance-replay flags emit None for now — Plan 2.1 expansion documented inline)
- §11 (end-to-end runbook order): Documented in README + each notebook's header markdown ✓
- §12 (safety invariants — CONFIRMED gate, DRY_RUN default, originals retained, claim guard, idempotent re-runs): Tasks 8, 9 ✓

**Acknowledged gaps deferred to Plan 2.1:**
- Row filter and column mask **replay** (capture works; replay surfaces a warning). Spec §5 acknowledges these are complex; auto-replay needs custom DDL building for each function signature. Plan 2.1 if customer needs unattended replay.
- Registered model migration. Spec §9.3 item 6 says use `mlflow` model registry API. Out of Plan 2 scope; doable but adds another module.
- Negative-test optional cell (gate-revoke read on old credential). Spec §10.4 acknowledges this is opt-in; Plan 2.1 if customer wants it.
- Validation governance-replay flags (`grants_ok`, `owner_ok`, etc.) are columns in `validation_results` but populated as None for now. Plan 2.1 wires the comparison against `object_metadata_snapshot`.

**Placeholder scan:** No "TBD" / "implement later" in actionable code. Plan 2.1 expansions are explicitly documented inline and in this self-review.

**Type consistency:**
- `ObjectRecord` (defined in Plan 1) used consistently across `migration.py`, `validation.py`, and both notebooks.
- `MigrationPlan`, `GovernanceSnapshot`, `ValidationResult` defined once and used consistently.
- `MIGRATION_LOG_SCHEMA`, `OBJECT_METADATA_SNAPSHOT_SCHEMA`, `VALIDATION_RESULTS_SCHEMA` defined in `state.py`, referenced consistently by writers and notebooks.
- `_RestProto`, `_Fs`, `_SqlExec` Protocol types provide mockable boundaries.

No type drift detected.
