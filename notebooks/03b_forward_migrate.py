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
# Identity values come from utils/config.py — edit there, not here.
from utils import config as _cfg
_cfg.resolve_config(spark=spark)
OLD_STORAGE_ACCOUNT = _cfg.OLD_STORAGE_ACCOUNT
NEW_STORAGE_ACCOUNT = _cfg.NEW_STORAGE_ACCOUNT
OPS_SCHEMA = _cfg.OPS_SCHEMA
ALLOW_MANAGED_VOLUMES_SKIP = _cfg.ALLOW_MANAGED_VOLUMES_SKIP

# Per-run operational gates — stay in this notebook so a single edit to
# utils/config.py can't arm destructive ops across multiple notebooks.
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
    check_external_location_for, probe_path_exists, probe_partition_completeness,
)
from utils.sql import quote_fqn
from utils.state import MigrationLog, SnapshotWriter
from utils.uc_client import ExternalLocationRecord


assert not (not DRY_RUN and not CONFIRMED), (
    "DRY_RUN=False requires CONFIRMED=True. Set both flags explicitly."
)

inv_df = spark.table(f"{OPS_SCHEMA}.inventory")
ext_locs_df = spark.table(f"{OPS_SCHEMA}.external_locations")
def _row_to_ext_loc(r) -> ExternalLocationRecord:
    """PySpark Row has no .get(); use asDict() so optional fields don't blow up."""
    d = r.asDict()
    return ExternalLocationRecord(
        name=d["name"], url=d["url"], credential_name=d["credential_name"],
        read_only=d["read_only"],
        region=d.get("region"),
        isolation_mode=d.get("isolation_mode"),
        accessible_in_current_workspace=d.get("accessible_in_current_workspace"),
    )


ext_locs = [_row_to_ext_loc(r) for r in ext_locs_df.collect()]
inv_rows = inv_df.collect()

drift = [r for r in inv_rows if r["classification"] == "drift_managed_on_old"]
external_old = [r for r in inv_rows if r["classification"] == "external_on_old"]
print(f"drift_managed_on_old: {len(drift)}")
print(f"external_on_old: {len(external_old)}")

# Fail-fast on managed volumes in scope. The repo cannot migrate them
# (Plan 2.1 scope). Customer must either handle them manually before
# running 03b, or explicitly opt to skip them via
# `ALLOW_MANAGED_VOLUMES_SKIP = True` in `utils/config.py`.
managed_volumes_in_drift = [
    r for r in drift
    if r["object_type"] == "VOLUME" and r["table_type"] == "MANAGED"
]
if managed_volumes_in_drift:
    print(f"\nFound {len(managed_volumes_in_drift)} managed volume(s) in drift:")
    for r in managed_volumes_in_drift[:10]:
        print(f"  {r['catalog']}.{r['schema']}.{r['name']}")
    if len(managed_volumes_in_drift) > 10:
        print(f"  ... and {len(managed_volumes_in_drift) - 10} more")

    if not ALLOW_MANAGED_VOLUMES_SKIP:
        raise RuntimeError(
            f"Managed volumes ({len(managed_volumes_in_drift)}) are in scope but the "
            "repo cannot migrate them (Plan 2.1 scope — manual handling needed: "
            "dbutils.fs.cp → DROP VOLUME → CREATE MANAGED VOLUME → replay grants). "
            "To proceed with table-only migration and skip volumes, set "
            "`ALLOW_MANAGED_VOLUMES_SKIP = True` in utils/config.py."
        )
    print("\nALLOW_MANAGED_VOLUMES_SKIP=True — proceeding with table-only migration. "
          "The listed managed volumes will be skipped.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Pre-flight: external location for new account + per-object data presence

# COMMAND ----------
fs = dbutils.fs  # noqa: F821

from utils.migration import rewrite_account_in_path

target_new_path = (
    rewrite_account_in_path(drift[0]["storage_path"], NEW_STORAGE_ACCOUNT, old_account=OLD_STORAGE_ACCOUNT)
    if drift else None
)
if target_new_path:
    el = check_external_location_for(target_path=target_new_path, external_locations=ext_locs)
    assert el is not None, f"No external location covers {target_new_path}"
    print(f"External location for new account: {el.name} ({el.credential_name})")

missing = []
partition_warnings = []
# Pre-flight presence check only applies to EXTERNAL objects — the migration
# for those is DROP+CREATE LOCATION at NEW, so the data must already be at
# NEW (e.g., via storage-layer azcopy). For drift_managed_on_old, the
# migration is DEEP CLONE which reads from OLD and writes to NEW as part of
# the migration step itself; pre-existing data at NEW would be incorrect
# (the clone target must be empty).
for r in external_old:
    if not r["storage_path"]:
        continue
    old_p = r["storage_path"]
    new_p = rewrite_account_in_path(old_p, NEW_STORAGE_ACCOUNT, old_account=OLD_STORAGE_ACCOUNT)
    if not probe_path_exists(fs=fs, path=new_p):
        missing.append((r["catalog"], r["schema"], r["name"], new_p))
        continue
    # Partition completeness: compare directory counts at old vs new for
    # tables/volumes that may be partitioned. probe_partition_completeness
    # treats new >= old (with old > 0) as complete; flags shortfalls.
    probe = probe_partition_completeness(fs=fs, old_path=old_p, new_path=new_p)
    if not probe.complete and probe.old_count > 0:
        partition_warnings.append(
            (r["catalog"], r["schema"], r["name"], probe.old_count, probe.new_count)
        )

if missing:
    print(f"\n{len(missing)} object(s) missing at new path:")
    for c, s, n, p in missing[:20]:
        print(f"  {c}.{s}.{n} -> {p}")
    if not DRY_RUN:
        raise RuntimeError("Pre-flight failed: complete the data copy before retrying.")

if partition_warnings:
    print(f"\n{len(partition_warnings)} object(s) with partition shortfall at new path:")
    for c, s, n, old_cnt, new_cnt in partition_warnings[:20]:
        print(f"  {c}.{s}.{n}: {new_cnt} entries at new vs {old_cnt} at old")
    if not DRY_RUN:
        raise RuntimeError(
            "Pre-flight failed: one or more objects have fewer entries at new than "
            "old. Finish the partitioned data copy before retrying."
        )

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
# Map plan-builder action names to the spec's status vocabulary
# (claimed → snapshot_taken → cloned → swapped → replayed → validated → failed).
_ACTION_TO_STATUS = {
    "clone": "cloned",
    "ctas": "cloned",
    "drop": "cloned",          # for external DROP+CREATE, drop is the first half
    "create": "cloned",        # full clone-equivalent completes on create
    "rename_orig": "cloned",   # intermediate step within swap; not a terminal state
    "rename_staging": "swapped",
}


def _execute_steps(rec: ObjectRecord, steps: list[tuple[str, str]]) -> None:
    for action, sql in steps:
        if DRY_RUN:
            print(f"    DRY [{action}]: {sql}")
        else:
            spark.sql(sql)
            status = _ACTION_TO_STATUS.get(action, action)
            mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name, status=status)


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

        plan = plan_external_table_migration(rec=rec, new_storage_account=NEW_STORAGE_ACCOUNT, old_storage_account=OLD_STORAGE_ACCOUNT)
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
        plan = plan_external_volume_migration(rec=rec, new_storage_account=NEW_STORAGE_ACCOUNT, old_storage_account=OLD_STORAGE_ACCOUNT)
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
# MAGIC ## Step 6.5 — Managed volumes (deferred to Plan 2.1)
# MAGIC
# MAGIC Managed volumes on old storage require: (1) physical data copy via
# MAGIC `dbutils.fs.cp` from old to new managed path, (2) DROP + CREATE MANAGED
# MAGIC VOLUME, (3) governance replay. Spec §9.3 item 5. This is Plan 2.1 scope —
# MAGIC surfaced here so they are not silently missed.

# COMMAND ----------
managed_volumes = [r for r in drift if r["object_type"] == "VOLUME" and r["table_type"] == "MANAGED"]
if managed_volumes:
    print(f"{len(managed_volumes)} managed volume(s) on old storage — deferred:")
    for r in managed_volumes:
        print(f"  {r['catalog']}.{r['schema']}.{r['name']}")
    print("\nThese need manual handling:")
    print("  1. dbutils.fs.cp(old_path, new_path, recurse=True)")
    print("  2. DROP VOLUME <fqn>")
    print("  3. CREATE MANAGED VOLUME <fqn>")
    print("  4. Replay grants via utils.governance.GovernanceReplayer")
else:
    print("No managed volumes on old storage.")

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
# MAGIC ## Next: run `04_validation`
# MAGIC
# MAGIC `__pre_migration` shadow tables are intentionally kept for rollback
# MAGIC safety. To remove them after validation passes + a grace period,
# MAGIC run the separate `05_cleanup` notebook (gated by both
# MAGIC `POST_VALIDATION_CLEANUP_OK` in `utils/config.py` AND
# MAGIC `DRY_RUN=False` in that notebook).
# MAGIC
# MAGIC Cleanup is irreversible — once shadows are dropped, `03a_rollback`
# MAGIC can no longer restore the original state.
