# UC Storage Migration — Customer Runbook

This is the canonical operating guide for running the `uc-storage-migration`
repo against a Unity Catalog managed catalog. It assumes the goal is to
move every managed Delta table (and external table where applicable) in
one or more catalogs from an OLD storage location to a NEW storage
location, with full evidence of correctness at the end.

The repo has been exercised end-to-end on AWS UC (S3) and uses
cloud-portable primitives, so the same flow applies on Azure UC (ADLS).
The runbook calls out cloud-specific bits where they exist.

---

## 0. Triage questions to answer before you start

These materially change the workflow. Answer them up front.

1. **What kind of catalog are you migrating?** Native UC managed catalog
   or HMS-federated catalog?
   - Run: `databricks catalogs get <name> -o json | jq .catalog_type`
   - `MANAGED_CATALOG` → native UC → you MUST run `00_repoint_schemas`
     before discovery. SQL `ALTER SCHEMA SET MANAGED LOCATION` is
     blocked on native UC; the repo uses the UC REST API instead.
   - Anything containing `FEDERATED` / HMS → SQL ALTER SCHEMA works
     and `00_repoint_schemas` is optional (the REST path also works
     for HMS-federated catalogs and is the recommended default).

2. **Do you have external tables in scope?** Quick check after running
   `01_discovery`:
   ```sql
   SELECT classification, count(*)
   FROM <ops_schema>.inventory
   WHERE classification IN ('external_on_old', 'external_on_new')
   GROUP BY classification;
   ```
   - `0` of both → migration is pure managed-Delta DEEP CLONE territory.
     The repo does ALL data movement; no storage-layer prep needed.
   - `> 0` external_on_old → those rows are NOT cloned. The repo does
     `DROP TABLE` + `CREATE EXTERNAL TABLE` at the new path, which
     requires the data to already be at NEW (typically via prior
     `azcopy`/`rsync`). Decide one of:
     - Re-verify or redo the storage-layer copy for external rows
     - Convert the externals to managed first, then DEEP CLONE handles them
     - Exclude them from this migration and handle separately

3. **Are there Materialized Views or Streaming Tables in scope?** The
   repo marks these as `requires_pipeline_handling=True` and SKIPS
   them. Their backing `__materialization_mat_*` Delta tables ARE
   migrated, so after the migration the MV definitions need a
   pipeline-owner `REFRESH MATERIALIZED VIEW`. Plan the coordination
   with the pipeline owners before running `03b_forward_migrate`.

4. **Do you have managed volumes in scope?** Currently deferred (Plan
   2.1). `03b_forward_migrate` **refuses to start** if any managed
   volumes are in drift, unless you set
   `ALLOW_MANAGED_VOLUMES_SKIP = True` in `utils/config.py`. With
   that flag set, 03b proceeds with table-only migration and the
   listed managed volumes are skipped. Manual handling: `dbutils.fs.cp`
   → `DROP VOLUME` → `CREATE MANAGED VOLUME` → replay grants.

5. **Do you have registered models or functions in scope?** `01_discovery`
   now enumerates them from `system.information_schema.models` and
   `.routines` and classifies them as `requires_external_handling`. The
   repo does not migrate them — they're catalog-scoped UC metadata that
   follows the catalog wherever it goes. They're surfaced in the
   inventory + decision report so you see them; no action is needed
   from the migration itself.

---

## 1. Setup (one-time)

1. **Pull the repo into the workspace.** Either clone via Repos UI, or
   use `databricks workspace import-dir` to push `utils/` + `notebooks/`
   side-by-side under a workspace folder you own.

2. **Edit `utils/config.py`.** This is the single source of truth.
   The notebooks call `resolve_config(spark=spark)` at startup, which
   fills in smart defaults you didn't set.

   Values you must set explicitly:
   - `OLD_STORAGE_ACCOUNT`, `NEW_STORAGE_ACCOUNT` — bare account/bucket
     names. For Azure this is the storage-account portion of the
     `abfss://` host. For AWS this is the bare bucket name.
   - `CATALOG_ALLOWLIST` — narrow down to the catalogs you actually
     want to migrate. **Empty list is refused** unless you also set
     `ALLOW_ALL_CATALOGS=True` (rarely correct for a production run).
   - `NEW_STORAGE_PREFIX` (for `00_repoint_schemas` only) — the full
     URL prefix where each schema's storage_root will be set, e.g.
     `s3://newbucket/migration/your_catalog` or
     `abfss://container@newacct.dfs.core.windows.net/migration/your_catalog`.

   Auto-derived defaults (leave as `None` to opt in):
   - `OPS_SCHEMA` → `f"{CATALOG_ALLOWLIST[0]}._migration_ops"`
   - `REPOINT_CATALOG` → `CATALOG_ALLOWLIST[0]` when there's exactly one
   - `SCHEMAS_TO_REPOINT` → all user-owned schemas in `REPOINT_CATALOG`,
     excluding `information_schema`, `default`, schemas starting with
     `_`, and the schema portion of `OPS_SCHEMA` if it lives there.

3. **Identify a compute target.** Serverless SQL warehouse works for
   most steps; the volume-size walker in `01_discovery` and the
   `_metadata.file_path` query in `04_validation` need a runtime that
   supports `dbutils.fs.ls` and Spark — i.e. a serverless notebook or
   all-purpose cluster, not a SQL-only warehouse. We've verified on
   serverless notebooks.

4. **Verify your permissions on the catalogs in scope.**
   - For ALL operations: `USE CATALOG` + `USE SCHEMA` + `SELECT` on every
     in-scope table (the repo reads everything during discovery /
     validation).
   - For `00_repoint_schemas`: catalog owner OR catalog `MANAGE`
     privilege (the REST PATCH on schemas requires it).
   - For `03b_forward_migrate`: `MODIFY` on tables, `CREATE TABLE` in
     each schema (for the staging clones), `ALTER TABLE RENAME`.
   - For external tables: `MANAGE` on the storage credential + external
     location for the NEW path.

---

## 2. Native UC catalogs: repoint schemas first

**Skip this section if your catalog is HMS-federated.**

UC native catalogs reject SQL `ALTER SCHEMA SET MANAGED LOCATION` with
`UC_COMMAND_NOT_SUPPORTED.NON_HMS_FEDERATED_ENTITY`. The underlying UC
REST API accepts the same change as a `PATCH` on the schema. The repo's
`00_repoint_schemas` notebook does this via the bundled Databricks SDK
(`w.api_client.do(method="PATCH", path="/api/2.1/unity-catalog/schemas/...", body={"storage_root": "..."})`).

What it does:

1. Reads each schema's current `storage_root`
2. PATCHes the schema's `storage_root` to `f"{NEW_STORAGE_PREFIX}/{schema}"`
3. Logs the before/after

What it does NOT do: move any existing tables. Per Databricks docs,
"Databricks does not move existing objects" — the repoint only affects
where NEW managed tables/volumes go. Existing tables stay at their
physical locations until `03b_forward_migrate` clones them.

**Run order:**

```
1. Open notebooks/00_repoint_schemas
2. Verify utils/config.py has REPOINT_CATALOG, SCHEMAS_TO_REPOINT,
   NEW_STORAGE_PREFIX set
3. Run with CONFIRMED=False — review the plan output
4. Set CONFIRMED=True, re-run — schemas are repointed
```

This is metadata-only and reversible at any point (PATCH back to the
old `storage_root` via the same API or the rollback notebook's Step 3).

---

## 3. Discovery — what's where, what classifies as what

`notebooks/01_discovery` builds the inventory. Writes:

- `<ops_schema>.inventory` — every UC object with its classification
- `<ops_schema>.external_locations` — registered ELs at run time
- `<ops_schema>.lineage_consumers` — downstream consumers (if
  `system.access.table_lineage` is enabled in your metastore)

Read-only against in-scope catalogs.

**Classification vocabulary:**

| Classification | Meaning | What 03b will do |
|---|---|---|
| `consistent_old` | obj on OLD, parent on OLD | Skip — no drift |
| `consistent_new` | obj on NEW, parent on NEW | Skip — already there |
| `drift_managed_on_old` | managed table, obj on OLD, parent on NEW | DEEP CLONE → RENAME swap |
| `external_on_old` | external table, obj on OLD | DROP + CREATE EXTERNAL at NEW |
| `external_on_new` | external table, obj already on NEW | Skip |
| `unknown_account` | obj on neither OLD nor NEW | Skip — needs human review |
| `path_missing` | view or no storage path | Skip |

**Verify after running:**

```sql
SELECT classification, count(*) FROM <ops_schema>.inventory GROUP BY classification ORDER BY 2 DESC;
```

You expect to see `drift_managed_on_old` as the dominant class. If you
see `unknown_account` for objects you expected to migrate, the most
common cause is `parent_managed_location` not matching the OLD account
exactly — usually a typo in `OLD_STORAGE_ACCOUNT` in `utils/config.py`.

**Size-collection note:** Managed Delta tables get sizes via
`DESCRIBE DETAIL`. Volumes get sizes via a bounded `dbutils.fs.ls`
walker (10,000 files / 30s budget per volume). Materialized Views /
Streaming Tables are skipped — `DESCRIBE DETAIL` rejects them — but
their backing `__materialization_mat_*` tables ARE sized. The
`## Size coverage` block in the decision report shows you the actual
coverage; treat byte totals as a lower bound.

---

## 4. Decision report — verdict + cost estimate

`notebooks/02_decision_report` reads `<ops_schema>.inventory` and emits:

- Verdict: `ROLLBACK_FEASIBLE` / `ROLLBACK_REQUIRES_SIGNOFF` /
  `FORWARD_MIGRATE_REQUIRED`
- Per-catalog classification breakdown
- Top catalogs by drift
- Pipeline-handling objects (MVs / streaming tables) — owners to
  coordinate with
- Unknown-account objects — needs human review
- **`## Size coverage` block** — sized / total per object-type, with
  explicit "lower bound" caveat
- Rollback-cost ledger (objects that would be dropped if you rolled
  back)
- Forward-migrate cost / time estimate

No state mutation.

---

## 5. Forward migrate — the actual data move

`notebooks/03b_forward_migrate`. Per-object plan:

- **Managed Delta** (`drift_managed_on_old`):
  1. `CREATE TABLE <staging> DEEP CLONE <orig>` — physically writes
     fresh files to the schema's NEW `storage_root`
  2. `ALTER TABLE <orig> RENAME TO <orig>__pre_migration` — original
     name now points at a shadow at OLD
  3. `ALTER TABLE <staging> RENAME TO <orig>` — new active name
     points at NEW data
  4. Row-count + schema-hash assertion (before == after)
  5. Replay grants/owner/tags/comments via `GovernanceReplayer`
- **External tables** (`external_on_old`):
  1. Capture grants/tags
  2. `DROP TABLE <orig>` (definition only — data is external)
  3. `CREATE EXTERNAL TABLE <orig> USING <fmt> LOCATION '<new_path>'`
  4. Replay governance

**Required flags:** `CONFIRMED=True` + `DRY_RUN=False`. The notebook
asserts that both are set.

**Recommended run order:**

```
1. DRY_RUN=True, CONFIRMED=False — read plan, sanity-check
2. DRY_RUN=False, CONFIRMED=True — actually execute
3. After 04_validation passes, set POST_VALIDATION_CLEANUP_OK=True
   in the SAME notebook and re-run to drop __pre_migration shadows
```

**Pre-flight checks:**

- For `external_on_old` rows: probes that data exists at the NEW path
  (`probe_path_exists`) and that partition counts match
  (`probe_partition_completeness`). If either fails, the run raises
  unless `DRY_RUN=True`. For external migrations, complete the storage
  copy before retrying.
- For `drift_managed_on_old` rows: NO pre-flight data check. DEEP CLONE
  creates the data at NEW as part of the step itself.

**Failure handling:**

- Per-object failures are logged to `<ops_schema>.migration_log` with
  `status='failed'` and a captured traceback.
- The run continues to the next object — one failure doesn't abort the
  batch.
- Common failures: DLT-owned internal tables (`event_log_*`) which the
  repo cannot clone (acceptable — they're not user data).
- Re-running the notebook is safe: each row is gated by a `claim()` in
  `migration_log` — already-validated rows are skipped.

**What's left behind after a successful run:**

- The active FQN (e.g. `bronze.customers`) now points at data at NEW.
- A `bronze.customers__pre_migration` shadow exists, pointing at the
  OLD data, for as long as you want to keep the rollback option.
- A row per object in `<ops_schema>.migration_log` with status,
  staging FQN, pre-migration FQN, row counts before/after, schema
  hashes.

---

## 6. Validation — four-layer evidence

`notebooks/04_validation` runs against every row in `migration_log`
with `status='validated'` and produces:

| Layer | Check | When it applies |
|---|---|---|
| 1 | `DESCRIBE TABLE EXTENDED` Location matches NEW | Always |
| 2 | `_delta_log/` exists at NEW via `dbutils.fs.ls` | External tables only — managed tables hit UC's `__unitystorage` block on `dbutils.fs.ls`; Layer 1 already proves the location for them |
| 3 | `SELECT _metadata.file_path` shows NEW paths | Always (uses `_metadata.file_path`; `input_file_name()` is rejected in UC) |
| 4 | Parent `managed_location` matches NEW | Managed tables only (external tables live where their location field says, regardless of parent) |

`overall_pass = True` iff every non-N/A layer is True.

Writes `<ops_schema>.validation_results` — one row per object with
flags, evidence JSON, timestamp.

---

## 7. Cleanup — separate notebook `05_cleanup`

After `04_validation` passes for every object and you've held the
grace period you want, cleanup runs as a standalone step:

1. Open `notebooks/05_cleanup`
2. In `utils/config.py`, set `POST_VALIDATION_CLEANUP_OK = True`
3. In the notebook, set `DRY_RUN = False`
4. Run

Both gates must be set; either alone produces a plan-only preview.
The notebook drops every `__pre_migration` table listed in
`migration_log` with `status='validated'` and writes an audit row to
`<OPS_SCHEMA>.cleanup_log` per drop.

**Cleanup is irreversible.** Once shadows are dropped, `03a_rollback`
can no longer restore the original state. The notebook prints a
warning before any drops execute.

Storage-layer files at OLD become orphans — delete them via the
storage console, or repoint the IAM role to deny access first
(recommended) so any forgotten readers fail visibly.

---

## 8. Rollback — `03a_rollback` is now a true inverse of `03b`

If you need to undo `03b_forward_migrate` (before running `05_cleanup`):

`notebooks/03a_rollback` reads `migration_log` to know exactly what
`03b` did, then inverts it:

| 03b did | 03a does |
|---|---|
| Managed Delta DEEP CLONE + RENAME swap | DROP migrated, RENAME `__pre_migration` shadow back to original FQN |
| Managed non-Delta CTAS + RENAME swap | Same as above |
| External table DROP + CREATE at new path | DROP migrated, CREATE EXTERNAL TABLE at the original `storage_path` from inventory |
| External volume DROP + CREATE at new path | Same as above (VOLUME variant) |

After per-object rollback, schema and catalog `storage_root` are
reverted to OLD via REST PATCH.

**Hard pre-condition**: every `__pre_migration` shadow recorded in
`migration_log` must still exist. If `05_cleanup` has already dropped
any of them, the notebook raises `RuntimeError` and refuses to start.
At that point rollback is impossible from this repo — restore from an
external backup or re-migrate from a known-good source.

After running, re-run `01_discovery` to verify everything is back to
`consistent_old`.

---

## 9. Audit trail

Everything the repo runs writes to `<ops_schema>`:

| Table | What |
|---|---|
| `inventory` | Discovery output — every UC object with classification (incl. REGISTERED_MODEL + FUNCTION as `requires_external_handling`) |
| `external_locations` | EL snapshot (incl. isolation_mode, accessible_in_current_workspace) |
| `lineage_consumers` | Downstream consumers (if system.access enabled) |
| `migration_log` | One row per object the migration touched: status, FQN, row counts, schema hashes, claim_by, error traces. `status='rolled_back'` after `03a_rollback`. |
| `object_metadata_snapshot` | Grants/owner/tags/filters/comments captured before each migration — used by `GovernanceReplayer` |
| `validation_results` | Per-object 4-layer evidence + overall_pass |
| `cleanup_log` | Created by `05_cleanup`. One row per dropped `__pre_migration` shadow (ts, fqn, status, error_message). |

For your own custom logging (test runs, evidence beyond the repo's
defaults), you can create your own schema with a VARIANT column and
write to it from any cell:

```sql
CREATE SCHEMA IF NOT EXISTS <catalog>._migration_logs MANAGED LOCATION '<new_path>';
CREATE TABLE <catalog>._migration_logs.run_log (
  run_id STRING, ts TIMESTAMP, phase STRING, check_name STRING,
  object_fqn STRING, status STRING, result VARIANT, notes STRING
) USING DELTA;
```

The migration test run that exercised this repo on AWS used exactly
that pattern.

---

## 10. Known limitations (and how to spot them)

1. **DLT-owned internal tables (`event_log_*`)** can't be cloned —
   surface as `FAILED` in `migration_log` with PERMISSION_DENIED.
   Acceptable: DLT manages its own state, not user data.
2. **Materialized Views / Streaming Tables** are flagged as
   `requires_pipeline_handling` and skipped. After migration completes,
   the pipeline owner must run `REFRESH MATERIALIZED VIEW` (or
   equivalent for STREAMING TABLE).
3. **Managed volumes** are deferred (Plan 2.1). Manual handling:
   `dbutils.fs.cp` → `DROP VOLUME` → `CREATE MANAGED VOLUME` → replay
   grants.
4. **External tables** require the data to already be at NEW. The repo
   does not move external-table files.
5. **`unknown_account` classification** can swallow real drift if
   `OLD_STORAGE_ACCOUNT` / `NEW_STORAGE_ACCOUNT` in `utils/config.py`
   don't exactly match the URL host portion. Sanity-check the inventory
   right after discovery.
6. **Region detection** is unreliable — UC's external-locations API
   does not return `region`. The decision report's cross-region cost
   estimate defaults to same-region GBPS unless you manually edit
   `ADLS_CLONE_GBPS_*` in `utils/config.py`. For a truly cross-region
   migration, set both knobs explicitly.

---

## 11. Getting help

The repo has been verified end-to-end on a real UC catalog (119/120
managed Delta tables migrated, 119/119 validated). The full session
findings — including every bug found and fixed during validation —
are in the git history; `git log --oneline` shows the customer-blocking
fixes as a sequence.

For repo issues, capture from the failing run:
- The notebook name + run ID
- The error message from the failing cell
- The current values of `OPS_SCHEMA`, `OLD_STORAGE_ACCOUNT`,
  `NEW_STORAGE_ACCOUNT`, and `CATALOG_ALLOWLIST` from `utils/config.py`
- The `migration_log` row for the affected object (if any)

For UC permission errors, the most-likely root causes:
- Schema isn't owned by you — REST PATCH needs ownership / MANAGE
- Catalog isolation_mode rejects the operation — check
  `databricks catalogs get <name>` for `ISOLATED` and the workspace
  binding
- Storage credential doesn't grant write to the NEW path — check
  via `databricks external-locations get <name>` and `databricks
  storage-credentials get <name>`
