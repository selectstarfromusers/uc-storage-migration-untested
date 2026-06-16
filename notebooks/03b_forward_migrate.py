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
import os
import sys


def _notebook_path() -> str | None:
    """Return the absolute workspace path of this notebook, or None.

    On Databricks serverless, sys.path[0] is the worker's tmp dir, not the
    notebook's workspace path — so we have to ask Databricks directly.
    """
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
        p = ctx.notebookPath().get()
        return f"/Workspace{p}" if p and not p.startswith("/Workspace") else p
    except Exception:
        return None


def _add_utils_to_path() -> bool:
    """Walk up looking for sibling utils/. Returns True if found.

    Tries (in order):
      1. The notebook's own workspace path via dbutils.notebook context.
      2. sys.path[0] (works when running standalone / via %run).
    Walks up either candidate looking for a sibling utils/ directory.
    Silent on success.
    """
    starts: list[str] = []
    nb = _notebook_path()
    if nb:
        starts.append(nb)
    if sys.path:
        starts.append(sys.path[0])
    starts.append(os.getcwd())

    for start in starts:
        candidate = start
        for _ in range(6):
            parent = os.path.dirname(candidate)
            if parent == candidate:
                break
            if os.path.isdir(os.path.join(parent, "utils")):
                if parent not in sys.path:
                    sys.path.insert(0, parent)
                return True
            candidate = parent
    return False


_found = _add_utils_to_path()

# Once utils is importable, an explicit REPO_ROOT_HINT in utils/config.py
# overrides — for the rare layout where utils/ is not a sibling of
# notebooks/. Customers set it ONCE in utils/config.py; every notebook
# picks it up here.
if _found:
    try:
        from utils.config import REPO_ROOT_HINT as _hint
        if _hint and _hint not in sys.path:
            sys.path.insert(0, _hint)
    except ImportError:
        pass
else:
    raise RuntimeError(
        "Could not auto-locate utils/ relative to this notebook. "
        "If utils/ isn't a sibling of notebooks/ in your workspace, edit "
        "this cell to add `sys.path.insert(0, '/Workspace/path/to/repo')` "
        "above this block (then set REPO_ROOT_HINT in utils/config.py so "
        "subsequent cells don't need the manual hack)."
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
# Identity values come from utils/config.py — edit there, not here.
import importlib
from utils import config as _cfg
importlib.reload(_cfg)  # pick up edits to utils/config.py without restarting Python
_cfg.resolve_config(spark=spark)
OLD_STORAGE_ACCOUNT = _cfg.OLD_STORAGE_ACCOUNT
NEW_STORAGE_ACCOUNT = _cfg.NEW_STORAGE_ACCOUNT
OPS_SCHEMA = _cfg.OPS_SCHEMA
ALLOW_MANAGED_VOLUMES_SKIP = _cfg.ALLOW_MANAGED_VOLUMES_SKIP
LARGE_VOLUME_FILE_THRESHOLD = _cfg.LARGE_VOLUME_FILE_THRESHOLD
VOLUME_COPY_PARALLELISM = _cfg.VOLUME_COPY_PARALLELISM
VOLUME_DISTRIBUTED_COPY = _cfg.VOLUME_DISTRIBUTED_COPY
VOLUME_DISTRIBUTED_COPY_PARTITIONS = _cfg.VOLUME_DISTRIBUTED_COPY_PARTITIONS

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
    build_create_managed_volume_sql,
    build_rename_volume_sql, compare_volume_listings,
    build_volume_copy_pairs, plan_resumable_volume_copy,
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

# Managed volumes in scope are migrated by Step 6.5 (staging-swap: create new
# managed volume → copy files → verify → rename swap → replay grants), unless
# `ALLOW_MANAGED_VOLUMES_SKIP = True` in `utils/config.py`, in which case they
# are listed and skipped (table-only migration).
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
    if ALLOW_MANAGED_VOLUMES_SKIP:
        print("\nALLOW_MANAGED_VOLUMES_SKIP=True — these managed volumes will be "
              "SKIPPED (table-only migration).")
    else:
        print("\nThese will be migrated in Step 6.5 (set ALLOW_MANAGED_VOLUMES_SKIP=True "
              "to skip). The runner needs CREATE VOLUME on the schema + OWNER on the volumes.")

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
# MAGIC ## Step 6.5 — Migrate managed volumes (staging-swap)
# MAGIC
# MAGIC Managed volumes have no `DEEP CLONE` and no `ALTER VOLUME SET LOCATION`.
# MAGIC Per volume: create a new managed volume (lands on the schema's repointed
# MAGIC new-storage location) → physically copy files in → **verify file
# MAGIC count/size/paths** → rename `orig`→`__pre_migration`, `staging`→`orig` →
# MAGIC replay grants. The original is retained as `__pre_migration` (dropped
# MAGIC later by `05_cleanup`), and an integrity mismatch **blocks** that volume
# MAGIC (staging dropped, original untouched, logged failed).
# MAGIC
# MAGIC Skipped entirely when `ALLOW_MANAGED_VOLUMES_SKIP=True`.

# COMMAND ----------
def _walk_volume(base):
    """Recursive (relpath, size) listing of every file under a /Volumes/ path.

    relpath is computed by locating `base` WITHIN each returned path, so it is
    robust to dbutils.fs.ls returning a `dbfs:` scheme prefix (and to the base
    name differing in length between the source and staging volumes).
    """
    base_norm = base.rstrip("/")
    out, stack = [], [base_norm]
    while stack:
        p = stack.pop()
        for e in dbutils.fs.ls(p):  # noqa: F821
            ep = e.path
            if ep.rstrip("/").endswith(base_norm):
                continue  # the listed dir itself
            if e.size == 0 and ep.endswith("/"):
                stack.append(ep)
            else:
                idx = ep.find(base_norm)
                rel = ep[idx + len(base_norm):].lstrip("/") if idx >= 0 else ep
                out.append((rel, e.size))
    return out


def _copy_files_threaded(pairs, parallelism):
    """Driver-side parallel copy of [(src, dst, rel, size), ...] over the FUSE
    /Volumes mount with shutil.copyfile.

    Mechanism matters more than thread count. Benchmarked on serverless
    (fevm-artm-dev, 2026-06-16), per-file throughput by copy mechanism:
        old dbutils.fs.cp(recurse=True) ......  4 files/s
        dbutils.fs.cp threaded @64 ..........  ~10-14 files/s (plateaus ~P64)
        shutil.copyfile FUSE threaded @512 ...  ~62 files/s  <-- this
    dbutils.fs.cp round-trips a service call per file and contends across
    threads; a local FUSE copy is 4-6x faster and keeps scaling with threads
    up to ~512 (plateaus there). Works on serverless (no executors needed).

    Per-file resumable skip (exists + matching size) so a re-run after a
    timeout only moves the remainder. Returns the count copied (incl. skips).
    Raises the first worker exception, so the volume is logged failed and
    staging is kept for a resumable re-run."""
    import os
    import shutil
    from concurrent.futures import ThreadPoolExecutor

    def _one(p):
        src, dst, _rel, sz = p
        if os.path.exists(dst) and os.path.getsize(dst) == sz:
            return
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)

    n = 0
    with ThreadPoolExecutor(max_workers=parallelism) as ex:
        for _ in ex.map(_one, pairs):
            n += 1
    return n


def _copy_files_distributed(pairs, partitions):
    """Opt-in copy across Spark EXECUTORS via local `/Volumes` FUSE writes.

    Faster on large all-purpose/dedicated clusters, but executor FUSE writes to
    UC Volumes are not guaranteed on every compute type (e.g. serverless) —
    gated behind VOLUME_DISTRIBUTED_COPY. Skips files already present at the
    target with a matching size, so it is itself resumable."""
    def _copy_part(rows):
        import os, shutil
        for src, dst, rel, sz in rows:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            if os.path.exists(dst) and os.path.getsize(dst) == sz:
                continue
            shutil.copyfile(src, dst)

    (spark.createDataFrame(pairs, "src string, dst string, rel string, sz long")
          .repartition(partitions)
          .foreachPartition(_copy_part))


def _migrate_one_managed_volume_copy(old_vol, new_vol, *, dry_run):
    """Copy phase of the managed-volume staging swap: walk source once, plan a
    resumable copy against whatever already landed in staging (from a prior
    timed-out run), copy the remainder via the configured mechanism, then walk
    the target once and verify count/size/paths. Returns (match, evidence).

    The source is walked exactly once and the target once (plus a cheap
    existing-staging walk that is empty on a first run) — no double walk of the
    546k-file source. On a verify mismatch the caller keeps staging so a re-run
    resumes; the original volume is never touched here."""
    src_listing = _walk_volume(old_vol)
    n_src = len(src_listing)
    # On a dry run the staging volume does not exist yet, so don't walk it.
    existing = [] if dry_run else _walk_volume(new_vol)  # partial on resume
    to_copy, done = plan_resumable_volume_copy(src_listing, existing)
    big = n_src >= LARGE_VOLUME_FILE_THRESHOLD
    mode = ("distributed" if VOLUME_DISTRIBUTED_COPY else "threaded") if big else "simple-recurse"
    print(f"    {n_src} source files ({sum(s for _, s in src_listing)} bytes); "
          f"{len(done)} already staged, {len(to_copy)} to copy; mode={mode}")
    if dry_run:
        return None, {"dry_run": True, "src_file_count": n_src, "to_copy": len(to_copy), "mode": mode}

    if to_copy:
        pairs = build_volume_copy_pairs(old_vol, new_vol, to_copy)
        if not big:
            # Small volume: proven low-overhead path.
            for src, dst, _rel, _sz in pairs:
                dbutils.fs.cp(src, dst)  # noqa: F821
        elif VOLUME_DISTRIBUTED_COPY:
            _copy_files_distributed(pairs, VOLUME_DISTRIBUTED_COPY_PARTITIONS)
        else:
            _copy_files_threaded(pairs, VOLUME_COPY_PARALLELISM)

    new_listing = _walk_volume(new_vol)
    return compare_volume_listings(src_listing, new_listing)


managed_volumes = [r for r in drift if r["object_type"] == "VOLUME" and r["table_type"] == "MANAGED"]
if not managed_volumes:
    print("No managed volumes on old storage.")
elif ALLOW_MANAGED_VOLUMES_SKIP:
    print(f"ALLOW_MANAGED_VOLUMES_SKIP=True — skipping {len(managed_volumes)} managed volume(s).")
else:
    for r in managed_volumes:
        rec = _row_to_record(r)
        if not mig_log.claim(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                             object_type="VOLUME", actor=ACTOR):
            continue
        try:
            snap = capturer.capture(catalog=rec.catalog, schema=rec.schema,
                                    name=rec.name, object_type="VOLUME")
            staging_c, staging_s, staging_n = derive_staging_fqn(rec.catalog, rec.schema, rec.name)
            pre_c, pre_s, pre_n = derive_pre_migration_fqn(rec.catalog, rec.schema, rec.name)
            old_vol = f"/Volumes/{rec.catalog}/{rec.schema}/{rec.name}"
            new_vol = f"/Volumes/{staging_c}/{staging_s}/{staging_n}"
            if DRY_RUN:
                print(f"  [DRY RUN] would migrate managed volume {rec.catalog}.{rec.schema}.{rec.name} "
                      f"(create {staging_n} → copy → verify → swap → replay)")
                # Preflight: surface the file count up front — a huge volume is
                # exactly what blows the job timeout, so operators see it here.
                _migrate_one_managed_volume_copy(old_vol, new_vol, dry_run=True)
                mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name, status="claimed")
                continue

            snap_writer.append(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                               object_type="VOLUME", snapshot_json=_serialize_snapshot(snap))
            # 1. New managed volume (lands on the schema's new-storage location).
            # Idempotent on a resumed run: a staging volume left by a prior
            # timed-out attempt is reused (CREATE ... IF NOT EXISTS), and the
            # copy below skips files already landed in it.
            spark.sql(build_create_managed_volume_sql(staging_c, staging_s, staging_n,
                                                      if_not_exists=True))
            # 2+3. Copy via the governed /Volumes/ paths (NOT the raw
            # __unitystorage location — UC denies direct dbutils.fs access:
            # LOCATION_OVERLAP) and verify count/size/paths. The copy is
            # parallel + resumable for large volumes; see
            # _migrate_one_managed_volume_copy.
            match, ev = _migrate_one_managed_volume_copy(old_vol, new_vol, dry_run=False)
            if not match:
                # Keep staging (do NOT drop) — the partial copy is the resume
                # point; re-running this notebook copies only the remainder.
                # The original is still untouched (no swap happened), so this
                # is safe. 05_cleanup / a manual DROP VOLUME removes the
                # staging volume if the migration is abandoned.
                raise RuntimeError(f"managed-volume integrity mismatch (staging KEPT for resume, "
                                   f"original untouched): {ev}")
            # 4. Swap: keep original as __pre_migration, promote staging.
            spark.sql(build_rename_volume_sql(rec.catalog, rec.schema, rec.name, pre_n))
            spark.sql(build_rename_volume_sql(staging_c, staging_s, staging_n, rec.name))
            # 5. Replay governance onto the promoted volume.
            replayer.replay(snap, target_fqn=(rec.catalog, rec.schema, rec.name), object_type="VOLUME")
            mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                           status="validated",
                           staging_fqn=f"{staging_c}.{staging_s}.{staging_n}",
                           pre_migration_fqn=f"{pre_c}.{pre_s}.{pre_n}")
            print(f"  migrated managed volume: {rec.catalog}.{rec.schema}.{rec.name} "
                  f"({ev['new_file_count']} files, {ev['new_total_bytes']} bytes)")
        except Exception as e:
            mig_log.update(catalog=rec.catalog, schema=rec.schema, name=rec.name,
                           status="failed", error_trace=traceback.format_exc())
            print(f"  FAILED managed volume {rec.catalog}.{rec.schema}.{rec.name}: {e}")

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
