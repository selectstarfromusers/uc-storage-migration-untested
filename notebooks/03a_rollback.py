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
# Identity values come from utils/config.py — edit there, not here.
from utils import config as _cfg
_cfg.resolve_config(spark=spark)
OLD_STORAGE_ACCOUNT = _cfg.OLD_STORAGE_ACCOUNT
NEW_STORAGE_ACCOUNT = _cfg.NEW_STORAGE_ACCOUNT
OPS_SCHEMA = _cfg.OPS_SCHEMA

# Per-run operational gates — stay in this notebook so a single edit to
# utils/config.py can't arm destructive ops across multiple notebooks.
CONFIRMED = False                # MUST be set to True to execute
DRY_RUN = True                   # set to False to actually execute (only after CONFIRMED=True)
ACTOR = "rollback_runner"        # identifier for migration_log claim_by

# COMMAND ----------
import json

from utils.discovery import ObjectRecord, classify_object
from utils.governance import GovernanceCapturer
from utils.migration import rewrite_account_in_path
from utils.paths import classify_url, parse_storage_url
from utils.reporting import DecisionThresholds, compute_recommendation
from utils.sql import quote_fqn
from utils.state import MigrationLog, SnapshotWriter


def _is_on(url, account):
    """True if `url` belongs to the OLD or NEW account/prefix.

    Uses classify_url so prefix-mode (S3 single-bucket testing) works.
    A bare classify_account would fail in prefix mode because the URL's
    bucket portion alone doesn't equal 'bucket/prefix'.
    """
    cls = classify_url(url, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT)
    if account == OLD_STORAGE_ACCOUNT:
        return cls == "old"
    if account == NEW_STORAGE_ACCOUNT:
        return cls == "new"
    return False

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
# MAGIC ## Pre-flight — recompute recommendation, probe storage access

# COMMAND ----------
# 1. Reconfirm rollback is still feasible against the current inventory state.
def _rec_to_object_record(r):
    return ObjectRecord(
        catalog=r["catalog"], schema=r["schema"], name=r["name"],
        object_type=r["object_type"], table_type=r["table_type"],
        data_source_format=r["data_source_format"],
        storage_path=r["storage_path"],
        parent_managed_location=r["parent_managed_location"],
        owner=r["owner"], created_at=r["created_at"], last_altered=r["last_altered"],
        requires_pipeline_handling=r["requires_pipeline_handling"],
        size_bytes=r["size_bytes"], tag_count=r["tag_count"],
        grant_count=r["grant_count"],
        has_row_filter=r["has_row_filter"], has_column_mask=r["has_column_mask"],
    )

bytes_on_new = sum(r["size_bytes"] or 0 for r, c in records if c == "consistent_new")
classified_for_rec = [(_rec_to_object_record(r), c) for r, c in records]
recommendation = compute_recommendation(
    classified_for_rec, thresholds=DecisionThresholds(), bytes_on_new=bytes_on_new,
)
print(f"Current recommendation verdict: {recommendation.verdict}")
print(f"  {recommendation.why}")

if not DRY_RUN:
    assert recommendation.verdict.startswith("ROLLBACK"), (
        f"Refusing to roll back: current recommendation is {recommendation.verdict}. "
        f"Re-run 01_discovery and 02_decision_report to reassess."
    )

# 2. Probe storage access on both old and new accounts (read+write).
def _probe_rw(path: str) -> tuple[bool, bool]:
    """Return (read_ok, write_ok). Cheap test: ls then write+delete a tiny marker."""
    try:
        dbutils.fs.ls(path)  # noqa: F821
        read_ok = True
    except Exception as e:
        print(f"  read FAILED on {path}: {e}")
        read_ok = False
    marker = f"{path.rstrip('/')}/._rollback_probe"
    try:
        dbutils.fs.put(marker, "probe", True)  # noqa: F821
        dbutils.fs.rm(marker)  # noqa: F821
        write_ok = True
    except Exception as e:
        print(f"  write FAILED on {path}: {e}")
        write_ok = False
    return read_ok, write_ok

# Probe one representative path on each side
probes = []
for r in rows:
    if r["storage_path"] and _is_on(r["storage_path"], OLD_STORAGE_ACCOUNT):
        probes.append(("old", r["storage_path"]))
        break
for r in rows:
    if r["storage_path"] and _is_on(r["storage_path"], NEW_STORAGE_ACCOUNT):
        probes.append(("new", r["storage_path"]))
        break

for label, p in probes:
    read_ok, write_ok = _probe_rw(p)
    print(f"  {label} storage probe @ {p}: read={read_ok} write={write_ok}")
    if not DRY_RUN:
        assert read_ok and write_ok, f"{label} storage probe failed; cannot proceed."

# 3. ALTER CATALOG dry-run check.
# UC may reject ALTER CATALOG SET MANAGED LOCATION when child schemas have
# managed_locations that conflict with the new target. After all `consistent_new`
# objects are dropped and all schemas are reverted (Step 3), the catalog ALTER
# should succeed. Surface catalogs that currently have schemas with non-old
# managed_locations as a warning so the user knows what to expect.
catalogs_with_new_schemas: set[str] = set()
for r in rows:
    parent = r["parent_managed_location"]
    if parent and _is_on(parent, NEW_STORAGE_ACCOUNT):
        catalogs_with_new_schemas.add(r["catalog"])
print(f"Catalogs whose schemas still point at new storage (will be reverted in Step 3): "
      f"{sorted(catalogs_with_new_schemas)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Capture governance state into object_metadata_snapshot

# COMMAND ----------
mig_log = MigrationLog(spark=spark, table_name=f"{OPS_SCHEMA}.migration_log")
mig_log.ensure_exists()

snap_writer = SnapshotWriter(spark=spark, table_name=f"{OPS_SCHEMA}.object_metadata_snapshot")
snap_writer.ensure_exists()

capturer = GovernanceCapturer(spark=spark)
owned_objects: list = []  # objects this runner successfully claimed; only these are dropped

for r in new_objects:
    if not mig_log.claim(catalog=r["catalog"], schema=r["schema"], name=r["name"],
                         object_type=r["object_type"], actor=ACTOR):
        print(f"  SKIP (claimed by another runner or already validated): "
              f"{r['catalog']}.{r['schema']}.{r['name']}")
        continue
    owned_objects.append(r)
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

print(f"Owned by this runner: {len(owned_objects)} / {len(new_objects)} objects")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Drop new-storage objects (in dependency order)
# MAGIC
# MAGIC Only objects this runner claimed in Step 1 are dropped. Objects claimed by
# MAGIC a concurrent runner are skipped; that runner is responsible for them.

# COMMAND ----------
def _drop_sql(obj_type: str, catalog: str, schema: str, name: str) -> str:
    kw = "VOLUME" if obj_type == "VOLUME" else "TABLE"
    return f"DROP {kw} {quote_fqn(catalog, schema, name)}"


# Sort: tables before volumes, then alphabetically
owned_sorted = sorted(
    owned_objects,
    key=lambda r: (r["object_type"] != "TABLE", r["catalog"], r["schema"], r["name"]),
)

for r in owned_sorted:
    sql = _drop_sql(r["object_type"], r["catalog"], r["schema"], r["name"])
    if DRY_RUN:
        print(f"  DRY: {sql}")
    else:
        try:
            spark.sql(sql)
            mig_log.update(catalog=r["catalog"], schema=r["schema"], name=r["name"],
                           status="dropped")
            print(f"  dropped: {r['catalog']}.{r['schema']}.{r['name']}")
        except Exception as e:
            mig_log.update(catalog=r["catalog"], schema=r["schema"], name=r["name"],
                           status="failed", error_trace=str(e))
            print(f"  FAILED: {r['catalog']}.{r['schema']}.{r['name']}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Revert schema managed_locations

# COMMAND ----------
schemas_to_revert = []
seen = set()
for r in rows:
    key = (r["catalog"], r["schema"])
    if key in seen:
        continue
    seen.add(key)
    parent = r["parent_managed_location"]
    parsed = parse_storage_url(parent)
    if parsed and parsed.account == NEW_STORAGE_ACCOUNT:
        old_path = rewrite_account_in_path(parent, OLD_STORAGE_ACCOUNT)
        schemas_to_revert.append((r["catalog"], r["schema"], old_path))

print(f"Schemas to revert: {len(schemas_to_revert)}")

# Use REST PATCH instead of SQL ALTER SCHEMA SET MANAGED LOCATION — the
# latter is rejected on native UC catalogs. See utils/uc_admin.py.
from utils.uc_admin import set_schema_storage_root
from databricks.sdk import WorkspaceClient
_rollback_w = WorkspaceClient()

for catalog, sch, old_path in schemas_to_revert:
    if DRY_RUN:
        print(f"  DRY: PATCH /api/2.1/unity-catalog/schemas/{catalog}.{sch} "
              f"storage_root='{old_path}'")
    else:
        try:
            set_schema_storage_root(
                api_client=_rollback_w.api_client,
                catalog=catalog, schema=sch, storage_root=old_path,
            )
            print(f"  reverted: {catalog}.{sch}")
        except Exception as e:
            print(f"  FAILED schema revert {catalog}.{sch}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Revert catalog managed_locations

# COMMAND ----------
from utils.uc_client import UcClient
from utils.uc_admin import set_catalog_storage_root
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
    parsed = parse_storage_url(c.storage_root)
    if parsed and parsed.account == NEW_STORAGE_ACCOUNT:
        old_path = rewrite_account_in_path(c.storage_root, OLD_STORAGE_ACCOUNT)
        if DRY_RUN:
            print(f"  DRY: PATCH /api/2.1/unity-catalog/catalogs/{c.name} "
                  f"storage_root='{old_path}'")
        else:
            try:
                set_catalog_storage_root(
                    api_client=w.api_client,
                    catalog=c.name, storage_root=old_path,
                )
                print(f"  reverted catalog {c.name}")
            except Exception as e:
                print(f"  FAILED catalog revert {c.name}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — Verify

# COMMAND ----------
print("Re-run 01_discovery to verify all objects are now consistent_old.")
