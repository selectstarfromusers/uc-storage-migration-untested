# UC Storage Reconciliation & Migration — Design Spec

- **Date:** 2026-05-12
- **Author:** Art Malanok (SA, Databricks)
- **Cloud:** Azure (ADLS Gen2)
- **Status:** Draft for customer review

---

## 1. Context

The customer attempted to migrate a Unity Catalog metastore's storage from one ADLS account to another but executed the steps out of order. Specifically:

- They copied data files from the old ADLS account to a new one (likely a full mirror with paths preserved, though this is not load-bearing for the design).
- They updated `managed_location` at the **metastore root** (possibly), **catalog**, and **schema** levels to point to the new ADLS account.
- They did **not** migrate existing UC objects from a metadata standpoint. Existing tables and volumes still reference `storage_location` paths on the **old** ADLS account.
- Any UC objects created **after** the `managed_location` change were written to the **new** ADLS account, because that is now the default managed location for new objects in those catalogs/schemas.

**Net result:** UC is in a split state. Existing objects point to old storage; new objects point to new storage. The customer needs to (a) understand precisely what is where, and (b) reconcile to a single, intentional state.

## 2. Goals

1. **Discover and classify** every UC object at every level (metastore → catalog → schema → table/volume/model/function) with its actual underlying storage location and whether it is on old, new, or other storage.
2. **Produce an opinionated recommendation**: roll back to old storage (if new-storage activity is negligible) or forward-migrate (if new-storage activity is substantial), with the supporting numbers.
3. **Provide best-practice migration playbooks** for each path, executable as Python notebooks the customer runs in their workspace as metastore admin.
4. **Provide irrefutable per-object verification** that, post-migration, queries against a given table physically read bytes from the desired storage account — provable through multiple independent layers of evidence.
5. **Preserve all governance state**: grants, owners, tags, row filters, column masks, comments, constraints. No silent data or governance loss.
6. **Resumability and safety**: every mutation logged; originals retained until customer-gated cleanup; per-object idempotency on re-run.

## 3. Non-goals

- Decommissioning the old storage account (customer decision after grace period).
- Migrating non-UC assets: Lakeview dashboards, Genie spaces, jobs, notebooks. These reference tables by FQN, which the migration preserves, so they continue to work without modification.
- Lakehouse Federation (foreign catalogs) — no storage to migrate.
- Delta Sharing recipients — separate communication; out of scope.
- Materialized views, streaming tables, DLT pipelines — flagged in discovery, handed to pipeline owners (full refresh after upstream tables migrate).

## 4. Constraints and assumptions

- **Cloud:** Azure ADLS Gen2 throughout. `abfss://<container>@<account>.dfs.core.windows.net/` URL syntax.
- **Form factor:** Python notebooks executed in the customer's workspace by a metastore admin (with account-admin escalation only for metastore-root operations, if needed).
- **Freeze window available:** customer can quiesce writes to migrating schemas during cutover. This unlocks the DEEP CLONE + RENAME-swap pattern, which is the simplest safe approach.
- **Data copy:** assumed to be a full mirror to the new account, but the design does not depend on this — pre-flight per-object probing handles partial or restructured copies.
- **Discovery backbone:** `system.information_schema.*` for bulk pulls + Databricks SDK for fields info_schema doesn't expose (catalog/schema `storage_root`) + REST APIs for external locations, storage credentials, metastore root.
- **Sizes:** `COLLECT_SIZES=True` by default to give the rollback decision real numbers. Customer can disable for speed.

## 5. Deliverable structure

Notebooks live under a project directory. Customer runs them in numbered order. State is durable in a Delta schema so re-runs are idempotent.

```
01_discovery.py              # Inventory and classify every UC object
02_decision_report.py        # Read-only: opinionated rollback vs forward recommendation
03a_rollback.py              # If chosen: revert managed_location, drop new-storage strays
03b_forward_migrate.py       # If chosen: migrate old-storage objects to new in safe order
04_validation.py             # Multi-layer evidence that migrated objects read from new
99_utils.py                  # Shared helpers (UC API client, FQN quoting, snapshot/replay)
```

Operational state schema (default `main._migration_ops`, configurable):

| Table | Purpose |
|---|---|
| `inventory` | One row per UC object: identity, storage path, classification, parent managed_location, raw `DESCRIBE EXTENDED` snippet |
| `external_locations` | UC-registered external locations + storage credentials per ADLS account |
| `lineage_consumers` | DLT pipelines / streaming jobs / recent queries that consume in-scope tables (from `system.access.table_lineage` + `system.access.audit`) |
| `object_metadata_snapshot` | Per-object pre-migration capture: grants, owner, tags, row filters, column masks, comments, constraints |
| `migration_log` | Per-object: `status`, `started_at`, `claimed_by`, `claimed_at`, `finished_at`, `row_count_before/after`, `schema_hash_before/after`, error trace |
| `validation_results` | Per-object multi-layer evidence rows |

Every notebook is parameterized by:

```python
OLD_STORAGE_ACCOUNT  = "oldacct"
NEW_STORAGE_ACCOUNT  = "newacct"
CATALOG_ALLOWLIST    = []                # empty = all catalogs in metastore
OPS_SCHEMA           = "main._migration_ops"
DRY_RUN              = False             # mutating notebooks only; default False
```

Every notebook starts with a markdown cell describing: **purpose**, **inputs read**, **outputs written**, **side effects on UC**, **how to re-run safely**, **how to roll back** (where applicable).

## 6. Phase A — Discovery (`01_discovery.py`)

### 6.1 Purpose

Produce a single, authoritative inventory of every UC object in scope, classified by which storage account it actually references, with enough auxiliary data to drive the rollback-vs-forward decision and to power the migration playbooks.

### 6.2 Data collection

1. **Catalogs.** `w.catalogs.list()` via SDK. Capture name, type, owner, `storage_root` (managed_location), isolation_mode, comment. Filter by `CATALOG_ALLOWLIST`. Skip `FOREIGN`, `DELTASHARING`, `SYSTEM` catalogs (flagged but excluded from migration scope).
2. **Schemas.** `w.schemas.list(catalog)` per catalog. Capture `storage_root` (managed_location) explicitly — info_schema doesn't expose it reliably.
3. **Tables.** `system.information_schema.tables` for `table_catalog`, `table_schema`, `table_name`, `table_type` (MANAGED / EXTERNAL / VIEW / MATERIALIZED_VIEW / STREAMING_TABLE), `data_source_format` (DELTA / ICEBERG / PARQUET / CSV / JSON / etc.), `created`, `last_altered`, `table_owner`, and `storage_path` where exposed. For any row where `storage_path` is null or absent (older metastore versions, certain table types), fall back to `DESCRIBE EXTENDED <fqn>` and parse the `Location` field. `99_utils.py` exposes a single `get_storage_path(fqn)` helper that handles the fallback transparently.
4. **Volumes.** `system.information_schema.volumes` for type (MANAGED / EXTERNAL), storage_location, owner, created_at.
5. **Registered models.** `system.information_schema.registered_models` (where available); fall back to `w.registered_models.list()` per schema. Capture storage_location (managed model artifacts live under schema managed_location).
6. **Functions/routines.** `system.information_schema.routines` — recorded for completeness; no storage; not in migration scope.
7. **External locations.** REST `GET /api/2.1/unity-catalog/external-locations`. Capture URL, credential_name, read_only flag.
8. **Storage credentials.** REST `GET /api/2.1/unity-catalog/storage-credentials`. Capture credential type (managed identity / SP / access connector), associated external locations.
9. **Metastore root.** REST `GET /api/2.1/unity-catalog/metastores/current`. Capture `storage_root` explicitly — needed to know whether metastore root was actually changed by the customer (likely not, since account-admin scope).
10. **Lineage consumers.** Query `system.access.table_lineage` for in-scope tables (downstream pipelines, streaming jobs, recent reads). Query `system.access.audit` for write activity. Store into `lineage_consumers` — informs which tables have hot consumers that need coordination.

### 6.3 Classification logic

Each UC object gets one `classification` value, computed from its actual storage path versus its parent's managed_location:

| Value | Definition |
|---|---|
| `consistent_old` | Object on **old** storage; parent managed_location also points to **old**. Untouched by the migration attempt. |
| `consistent_new` | Object on **new** storage; parent managed_location also points to **new**. Either born after the change or already migrated. |
| `drift_managed_on_old` | **Problem case.** Managed object still on **old** storage, but its parent (catalog or schema) managed_location was changed to **new**. The customer's incomplete migration. |
| `external_on_old` | External object physically on **old** storage. Independent of parent. Forward-migrate via DROP+CREATE at new path. |
| `external_on_new` | External object physically on **new** storage. No action. |
| `unknown_account` | Storage path resolves to neither old nor new (e.g., third storage account, or unparseable). Human review. |
| `path_missing` | Could not read storage_path. Views, SQL functions, and tables with permission issues. Excluded from migration. |

Each object also gets:

- `requires_pipeline_handling: bool` — `True` for `MATERIALIZED_VIEW` and `STREAMING_TABLE` table_types and for tables that appear as outputs of a DLT pipeline in lineage.
- `data_source_format` — used by `03b` to branch DEEP CLONE (Delta) vs CTAS (non-Delta).
- `has_row_filter`, `has_column_mask`, `tag_count`, `grant_count` — used to gauge governance-replay complexity per object.

### 6.4 Outputs

- `_migration_ops.inventory` Delta table (one row per UC object with ~30 columns)
- `_migration_ops.external_locations` Delta table
- `_migration_ops.lineage_consumers` Delta table
- A markdown summary cell printed at notebook end: counts per classification, counts per catalog, top 10 catalogs by `drift_managed_on_old` count, list of `unknown_account` rows for review, count of `MATERIALIZED_VIEW`/`STREAMING_TABLE` objects flagged for pipeline-owner coordination.

### 6.5 Edge cases handled explicitly

- Tables in `hive_metastore` catalog (legacy) — flagged with `classification=path_missing` (not UC) and excluded.
- Tables with names containing reserved words or special characters — all generated SQL goes through a backtick-quoting helper in `99_utils.py`.
- Catalogs with `isolation_mode=ISOLATED` — captured but no special handling at discovery; relevant for migration if storage credential bindings differ.
- ADLS regions per storage account — captured (via SDK `w.external_locations.get`) for the cross-region egress cost flag in `02`.

## 7. Phase A.5 — Decision report (`02_decision_report.py`)

### 7.1 Purpose

Read-only analysis of `_migration_ops.inventory` that prints an opinionated recommendation: **rollback feasible**, **rollback requires sign-off**, or **forward-migrate required**. No mutations.

### 7.2 Thresholds (tunable in config cell)

```python
ROLLBACK_THRESHOLDS = {
    "max_consistent_new_objects":      25,
    "max_bytes_on_new_gb":              10,
    "max_distinct_owners_on_new":        3,
    "max_age_days_on_new":               30,    # if any new-storage object is older → forward
}
```

### 7.3 Output structure

The notebook prints three markdown blocks:

1. **State summary table** — counts per classification, broken out by catalog. Highlights `drift_managed_on_old` (the migration backlog) and `consistent_new` (the rollback cost).

2. **Rollback-cost ledger** — every `consistent_new` object listed with: full name, owner, created_at, size, naming pattern flag (matches `_test_`, `_tmp_`, `_sandbox_`, `_scratch_` regex). The "if you rollback, here's exactly what gets dropped" list.

3. **Cost and time estimate** for the forward path:
   - Total bytes to clone (sum of `drift_managed_on_old` sizes)
   - Estimated clone duration (rule-of-thumb GB/s for ADLS-to-ADLS, adjusted for cross-region if old/new accounts are in different regions)
   - Estimated DBU cost (clone is full data copy)
   - **Cross-region egress flag** if applicable

4. **Recommendation** — one of:
   - `ROLLBACK FEASIBLE` (all thresholds satisfied)
   - `ROLLBACK REQUIRES SIGN-OFF` (some thresholds tripped but objects appear throwaway by naming)
   - `FORWARD-MIGRATE REQUIRED` (real workloads on new — rollback would lose work)

Each recommendation includes a one-paragraph "why" quoting the specific numbers, so the customer can defend the decision.

## 8. Phase B-Rollback — `03a_rollback.py`

### 8.1 Purpose

If recommendation is `ROLLBACK FEASIBLE` or `ROLLBACK REQUIRES SIGN-OFF` and the customer accepts, revert the configuration changes and drop the small set of new-storage objects, returning the metastore to its pre-attempt state.

### 8.2 Pre-flight (cell 1)

- Re-load `_migration_ops.inventory` (don't trust stale state)
- Recompute recommendation; **abort** if no longer in a rollback-eligible state
- Require `CONFIRMED = True` config flag
- Probe read+write access to both old and new ADLS accounts
- **`ALTER CATALOG SET MANAGED LOCATION` dry-run check**: for each catalog whose managed_location must revert, validate via SDK that the operation will succeed given current children. If UC rejects, surface the rejection and document the workaround (typically: re-order schemas first, or recreate catalog as a last resort).
- **Metastore root state check**: read current metastore `storage_root` via REST. Only attempt revert if it actually differs from the old account.

### 8.3 Steps

1. **Snapshot governance state** for every `consistent_new` object into `object_metadata_snapshot` — grants, owner, tags, row filters, column masks, comments. Kept indefinitely as audit trail even after drop. Also captures catalog/schema-level grants for any schema that exists only on new (about to be dropped).

2. **Drop new-storage objects** in dependency order: views → tables → volumes → registered models → schemas-that-only-exist-on-new. Each drop logged to `migration_log` with full metadata.

3. **Revert schema managed_locations** — for each schema whose `managed_location` points to new, issue `ALTER SCHEMA <fqn> SET MANAGED LOCATION '<old equivalent>'`.

4. **Revert catalog managed_locations** — same pattern, after all child schemas have been reverted.

5. **Revert metastore root** (account-admin step, conditional) — if metastore root was actually changed, REST `PATCH /api/2.1/unity-catalog/metastores/<id>`. Notebook detects whether caller has account-admin scope; if not, prints the exact REST call for the account admin to run out-of-band.

6. **Verify** — re-run discovery's classification logic in place. Success criterion: zero `consistent_new`, zero `drift_managed_on_old`, every remaining object `consistent_old`. Otherwise abort and dump offending rows.

### 8.4 Known gotcha

`ALTER CATALOG SET MANAGED LOCATION` is rejected when children's locations are incompatible. Pre-flight schemas-first ordering avoids most cases; the dry-run check catches the rest before any mutation.

## 9. Phase B-Forward — `03b_forward_migrate.py`

### 9.1 Purpose

Move every `drift_managed_on_old` and `external_on_old` object to new storage, in the right sequence, with per-object freeze windows and validation gates. Resumable on failure; idempotent on re-run.

### 9.2 Pre-flight (cell 1)

- External location for the new ADLS account exists and is healthy (`READ_FILES` + `WRITE_FILES` probe via `dbutils.fs.ls` on a known path).
- Storage credential bound to the new external location is valid.
- Read access to old ADLS account still works (clone reads from old during copy).
- **Per-object data presence probe** at the new path, including **partition completeness check** for partitioned tables (compare directory listings at old vs new, or sampled file-count parity per partition). Any object whose data isn't yet at new → **fail loudly with the list**; customer finishes the data copy before retry.
- `CONFIRMED = True` config flag.
- `DRY_RUN` honored: prints every SQL / API call the migration would issue, executes none.

### 9.3 Migration order (cheap and safe → expensive and risky)

1. **External tables on old** → DROP + CREATE EXTERNAL TABLE at new path. Grants/tags/row filters/column masks/owner/comments replayed from snapshot.

2. **External volumes on old** → DROP + CREATE EXTERNAL VOLUME at new path. Grants replayed.

3. **Managed Delta tables (`drift_managed_on_old`, format=DELTA)** — the heavy lift, see 9.4.

4. **Managed non-Delta tables (`drift_managed_on_old`, format≠DELTA)** — CTAS pattern: `CREATE TABLE staging AS SELECT * FROM original`, then validate row count and schema, then RENAME swap. No time-travel preservation.

5. **Managed volumes on old** → physical copy via `dbutils.fs.cp` if not already present, then DROP + CREATE MANAGED VOLUME. Grants replayed.

6. **Registered models** with managed artifact paths → use `mlflow` model registry API to re-register at new location.

7. **Materialized views / streaming tables** → **not auto-migrated**. Notebook prints the list and hands off to pipeline owners (full refresh after upstream parents are migrated).

### 9.4 Per managed Delta table playbook

```
for each table in drift_managed_on_old (format=DELTA):
  1. Claim row in migration_log (CAS-style insert; conflict → fail with claimer identity)
  2. Capture object_metadata_snapshot:
       - grants (SHOW GRANTS)
       - owner (DESCRIBE TABLE EXTENDED)
       - tags (SHOW TAGS — table + columns)
       - row filter (SHOW ROW FILTER) and column masks (SHOW COLUMN MASK)
       - comments (table + column)
       - constraints (informational PK/FK, CHECK)
  3. Capture row_count_before, schema_hash_before → migration_log
  4. Announce freeze: notebook prints a "freeze window now for <fqn>" banner;
     customer pauses writers (script does not enforce, only documents)
  5. CREATE TABLE <fqn>__migrate_staging DEEP CLONE <fqn>
     (no LOCATION clause — schema's managed_location places it on new automatically)
  6. Validate staging:
       - row_count match
       - schema_hash match
       - DESCRIBE DETAIL location on new account
       - input_file_name() sample shows reads from new account
  7. ALTER TABLE <fqn> RENAME TO <fqn>__pre_migration
  8. ALTER TABLE <fqn>__migrate_staging RENAME TO <fqn>
     (steps 7 and 8 in a single cell, no logic between, to minimize unavailability window)
  9. Replay governance from snapshot:
       - GRANT ... TO ... for each captured grant
       - ALTER TABLE <fqn> OWNER TO <captured owner>
         (fallback to current user if captured principal no longer exists, log substitution)
       - SET TAGS, SET ROW FILTER, SET COLUMN MASK, SET COMMENT
  10. Final validation: re-run #6 against new <fqn>
  11. Update migration_log: status=validated, finished_at, row_count_after, schema_hash_after
  12. (deferred) DROP TABLE <fqn>__pre_migration — gated cleanup cell run after grace period
```

### 9.5 Per managed non-Delta table playbook

Same as 9.4 except step 5 is `CREATE TABLE <fqn>__migrate_staging AS SELECT * FROM <fqn>` (CTAS). Time-travel history is not preserved. Validation includes explicit row-count and column-by-column schema comparison since CTAS doesn't auto-preserve all properties.

### 9.6 Idempotency, resumability, isolation

- Every step writes to `migration_log` with a `status` field: `claimed`, `snapshot_taken`, `cloned`, `swapped`, `replayed`, `validated`, `failed`.
- Re-running the notebook skips any object with `status=validated`; resumes mid-pipeline for any intermediate status.
- `--retry-failed-only` mode (config flag) reprocesses only `failed` rows.
- Per-object failures log and continue; final summary lists all failures with stack traces and the exact resume command.
- **Concurrent-run guard**: the claim in step 1 prevents two engineers from racing on the same object. Conflict surfaces the other claimer's identity.

### 9.7 Critical safety

Originals are **renamed, never dropped** during migration — `<fqn>__pre_migration` is retained until the gated cleanup cell. Per-table rollback is two ALTERs:

```sql
ALTER TABLE <fqn> RENAME TO <fqn>__rollback;
ALTER TABLE <fqn>__pre_migration RENAME TO <fqn>;
```

This works for any individual migrated table during the grace period.

## 10. Phase C — Validation (`04_validation.py`)

### 10.1 Purpose

For every migrated object, produce **layered, independent evidence** that queries genuinely read from new storage. No single proof relies on the layer below it.

### 10.2 Four-layer evidence model

| # | Proof | What it shows | Independence |
|---|---|---|---|
| 1 | `DESCRIBE EXTENDED <fqn>` → parse `Location` host | UC metadata says the table lives at new path | UC catalog state |
| 2 | `dbutils.fs.ls('<new_path>/_delta_log')` returns recent `*.json` (Delta) or directory listing matches storage_path (non-Delta) | Transaction log / files physically at new path | Storage filesystem state |
| 3 | `SELECT DISTINCT input_file_name() FROM <fqn>` sampled with `LIMIT 10000` (or per-partition for very large tables) | Spark actually reads bytes from new account at query time | **Runtime-observed** — bypasses all metadata layers |
| 4 | Host parsed from #3 matches parent `managed_location` host (managed) or expected external root (external) | Storage location is in the right hierarchy, not just *some* path on new | Cross-reference of two independent sources |

A migration is `overall_pass = True` only when **all four** layers agree. Any single mismatch → `failed` with the offending evidence captured verbatim into `validation_results`.

### 10.3 Governance-replay validation

In addition to storage layers, validate that governance state was correctly replayed:

- Grants on migrated `<fqn>` match `object_metadata_snapshot`
- Owner matches snapshot (or recorded substitution)
- Tags, row filters, column masks, comments match snapshot

Differences logged into `validation_results` as separate columns (`governance_grants_ok`, `governance_owner_ok`, etc.).

### 10.4 Optional negative test (`RUN_NEGATIVE_TEST=False` by default)

For a sampled subset, temporarily revoke read on the old storage credential, run a count query, confirm it succeeds. Most aggressive irrefutable proof — read from old physically cannot satisfy the query. Restores access immediately after. Gated because it briefly affects anything else still reading old.

### 10.5 Volume validation

`DESCRIBE VOLUME EXTENDED <fqn>` returns backing path; `dbutils.fs.ls` on `/Volumes/<catalog>/<schema>/<volume>/` returns files; assert backing path host matches new account.

### 10.6 Standalone verification function

`99_utils.py` exposes `verify_object_on_new(fqn) -> ValidationResult` that the customer can call ad-hoc indefinitely after migration completes — supports their "prove it to me" requests permanently.

### 10.7 Output

- `_migration_ops.validation_results` Delta table (one row per object with all evidence fields and raw outputs)
- Markdown summary: pass/fail counts per catalog, exhaustive failure list with evidence diffs

## 11. End-to-end runbook order

1. `01_discovery.py` — inventory state.
2. `02_decision_report.py` — get recommendation; customer reviews and chooses path.
3. (Optional pilot) Set `CATALOG_ALLOWLIST=["<one_small_catalog>"]` and run `03b` end-to-end on a single low-risk catalog. Validate. Then widen scope.
4. `03a_rollback.py` **or** `03b_forward_migrate.py`.
5. `04_validation.py` — confirm every migrated object passes all four evidence layers.
6. Re-run `01_discovery.py` — confirm zero `drift_managed_on_old` remains, inventory matches expectations.
7. Customer-decided grace period (recommended minimum 7 days; longer if streaming consumers exist).
8. Run gated cleanup cell in `03b` to drop `<fqn>__pre_migration` tables.

## 12. Safety invariants

- **Nothing is destroyed until validation passes.** Originals renamed, not dropped. Drop is a separate, gated step.
- **Governance state captured before any mutation** and stored durably in `object_metadata_snapshot` — survives notebook restarts, replayable independently.
- **Every mutation writes to `migration_log` before and after.** A killed notebook leaves a state row, not silent state.
- **Idempotent re-runs.** Already-completed objects are skipped; intermediate states resume from the right step.
- **Per-object isolation.** One object's failure does not abort the batch.
- **Concurrent-run guard.** First claimer wins; second sees claimer identity and aborts.
- **Pilot mode.** `CATALOG_ALLOWLIST` lets the customer prove the playbook on a low-risk catalog before going wide.
- **`DRY_RUN` mode** in every mutating notebook prints planned operations without executing.

## 13. Known risks and operational warnings

| Risk | Mitigation |
|---|---|
| RENAME-swap window — brief "table not found" gap | Both ALTERs issued in a single cell with no intervening logic; documented unavailability window during freeze |
| Partial / restructured data copy at new account | Per-object data presence probe + partition completeness check in pre-flight |
| Streaming / Auto Loader checkpoint references to old paths | Discovery surfaces consumers via `lineage_consumers`; cleanup phase reminds customer to coordinate checkpoint resets |
| Orphaned principals (deleted users / SPs) as captured owners | Owner-replay falls back to current notebook executor and logs substitution |
| FQN special characters / reserved words | All generated SQL routed through backtick-quoting helper in `99_utils.py` |
| Cross-region egress costs | Discovery captures region per external location; decision report flags cross-region migration with cost estimate |
| `ALTER CATALOG SET MANAGED LOCATION` rejected by UC | Rollback pre-flight dry-run check; documented workaround if rejected |
| Non-Delta managed tables (Iceberg, etc.) | Branch in `03b` to CTAS pattern; no time-travel preservation |
| Two engineers running migrate simultaneously | `migration_log` claim row with `claimed_by` / `claimed_at`; CAS-style insert |

## 14. Out of scope (called out so they don't ambush)

- Materialized views, streaming tables, DLT pipelines — flagged in discovery, handled separately with pipeline owners.
- Delta Sharing recipients still pointing at the old metastore root — separate communication.
- Lakehouse Federation (foreign catalogs) — no storage to migrate.
- Lakeview dashboards / Genie spaces — references update automatically via FQN preservation.
- Old-storage decommissioning — customer's call after grace period and audits.
- Hive_metastore catalog — out of UC scope; flagged in discovery, excluded from migration.

## 15. Open decisions for customer

1. **Pilot catalog choice.** Which low-risk catalog runs end-to-end first to prove the playbook?
2. **Freeze window cadence.** Per-schema or per-catalog? Length expectations once size estimate is in hand.
3. **Grace period length.** Minimum 7 days, but longer if streaming consumers need extended coexistence.
4. **Cleanup authority.** Who runs the gated cleanup cell and when?
5. **Negative test opt-in.** Customer can request `RUN_NEGATIVE_TEST=True` for the strongest irrefutable proof on a sampled set; default is off.

---

*End of design spec.*
