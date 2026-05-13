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
