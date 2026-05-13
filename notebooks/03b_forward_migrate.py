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

target_new_path = (
    drift[0]["storage_path"].replace(f"@{OLD_STORAGE_ACCOUNT}.", f"@{NEW_STORAGE_ACCOUNT}.", 1)
    if drift else None
)
if target_new_path:
    el = check_external_location_for(target_path=target_new_path, external_locations=ext_locs)
    assert el is not None, f"No external location covers {target_new_path}"
    print(f"External location for new account: {el.name} ({el.credential_name})")

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
        snap = capturer.capture(catalog=rec.catalog, schema=rec.schema, name=rec.name)
        if not DRY_RUN:
            snap_writer.append(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                               object_type="TABLE", snapshot_json=_serialize_snapshot(snap))

        orig_fqn = quote_fqn(rec.catalog, rec.schema, rec.name)
        row_count_before = spark.sql(f"SELECT count(*) AS k FROM {orig_fqn}").collect()[0]["k"]
        schema_hash_before = hash(tuple((f.name, f.dataType.simpleString())
                                        for f in spark.table(orig_fqn).schema.fields))

        plan = plan_managed_delta_migration(rec=rec)
        _execute_steps(rec, plan.steps)

        if not DRY_RUN:
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
