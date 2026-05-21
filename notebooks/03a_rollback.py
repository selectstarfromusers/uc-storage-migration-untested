# Databricks notebook source
# MAGIC %md
# MAGIC # 03a_rollback — Undo a `03b_forward_migrate` run
# MAGIC
# MAGIC **Purpose:** True inverse of `03b_forward_migrate`. For every object
# MAGIC the migration touched, restore the original state:
# MAGIC - Managed tables: DROP the migrated (at NEW), RENAME the
# MAGIC   `__pre_migration` shadow back to the original FQN.
# MAGIC - External tables: DROP the migrated, CREATE EXTERNAL TABLE at the
# MAGIC   original location captured in `inventory.storage_path`.
# MAGIC - External volumes: DROP the migrated, CREATE EXTERNAL VOLUME at
# MAGIC   the original location.
# MAGIC - Revert each schema's and catalog's `storage_root` to OLD via the
# MAGIC   UC REST API (SQL ALTER SCHEMA SET MANAGED LOCATION is blocked on
# MAGIC   native UC).
# MAGIC
# MAGIC **Inputs:** `<OPS_SCHEMA>.migration_log` (what 03b did) +
# MAGIC `<OPS_SCHEMA>.inventory` (the pre-migration state).
# MAGIC
# MAGIC **Outputs:** Per-object operations logged into
# MAGIC `<OPS_SCHEMA>.migration_log` with `status='rolled_back'`.
# MAGIC
# MAGIC **HARD PRE-CONDITION:** `__pre_migration` shadow tables must still
# MAGIC exist. If `05_cleanup` has already dropped them, rollback is
# MAGIC IMPOSSIBLE from this repo — restore from a separate backup. The
# MAGIC notebook refuses to start when any shadow is missing.
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

# Per-run operational gates — stay in this notebook.
CONFIRMED = False                # MUST be True to actually execute
DRY_RUN = True                   # set False to apply (only after CONFIRMED=True)
ACTOR = "rollback_runner"        # identifier for migration_log claim_by

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------
import json

from utils.discovery import ObjectRecord
from utils.governance import GovernanceCapturer
from utils.migration import rewrite_account_in_path
from utils.paths import parse_storage_url
from utils.sql import quote_fqn
from utils.state import MigrationLog
from utils.uc_admin import set_schema_storage_root, set_catalog_storage_root
from utils.uc_client import UcClient
from databricks.sdk import WorkspaceClient


assert not (not DRY_RUN and not CONFIRMED), (
    "DRY_RUN=False requires CONFIRMED=True. Set both flags explicitly."
)

w = WorkspaceClient()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Read migration_log + inventory, identify rollback set

# COMMAND ----------
# Join migration_log (what 03b did) with inventory (pre-migration state)
# to know each object's original storage_path for external recreate.
rollback_rows = spark.sql(f"""
SELECT
  m.catalog, m.schema, m.name, m.object_type, m.pre_migration_fqn,
  m.status AS mig_status,
  i.table_type, i.data_source_format, i.storage_path AS original_storage_path
FROM {OPS_SCHEMA}.migration_log m
LEFT JOIN {OPS_SCHEMA}.inventory i
  ON m.catalog = i.catalog AND m.schema = i.schema AND m.name = i.name
WHERE m.status IN ('validated', 'cloned', 'swapped', 'replayed', 'snapshot_taken')
""").collect()

print(f"Rollback candidates: {len(rollback_rows)} object(s) recorded in migration_log")
for r in rollback_rows[:5]:
    print(f"  {r['catalog']}.{r['schema']}.{r['name']}  "
          f"type={r['object_type']}/{r['table_type']}  status={r['mig_status']}")
if len(rollback_rows) > 5:
    print(f"  ... and {len(rollback_rows) - 5} more")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — HARD PRE-CONDITION: shadows must exist
# MAGIC
# MAGIC For every managed-table rollback to work, the `__pre_migration`
# MAGIC shadow must still exist (i.e., `05_cleanup` has not run for that
# MAGIC row). If any are missing, refuse to proceed.

# COMMAND ----------
def _table_exists(fqn: str) -> bool:
    """True if the table exists in UC."""
    if not fqn:
        return False
    try:
        parts = fqn.replace("`", "").split(".")
        if len(parts) != 3:
            return False
        cat, sch, name = parts
        n = spark.sql(
            "SELECT count(*) AS n FROM system.information_schema.tables "
            f"WHERE table_catalog = '{cat}' AND table_schema = '{sch}' "
            f"AND table_name = '{name}'"
        ).collect()[0]["n"]
        return int(n) > 0
    except Exception:
        return False


missing_shadows = []
for r in rollback_rows:
    if r["pre_migration_fqn"]:
        if not _table_exists(r["pre_migration_fqn"]):
            missing_shadows.append(r["pre_migration_fqn"])

if missing_shadows:
    print(f"\n{len(missing_shadows)} __pre_migration shadow(s) are MISSING:")
    for fqn in missing_shadows[:10]:
        print(f"  {fqn}")
    if len(missing_shadows) > 10:
        print(f"  ... and {len(missing_shadows) - 10} more")
    raise RuntimeError(
        "Rollback is impossible from this repo: __pre_migration shadows "
        "have been dropped (likely by 05_cleanup). The original tables "
        "no longer exist in UC. Restore from an external backup if you "
        "have one, or re-migrate from a known-good source."
    )

print(f"All {sum(1 for r in rollback_rows if r['pre_migration_fqn'])} expected shadow(s) present.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Per-object rollback

# COMMAND ----------
mig_log = MigrationLog(spark=spark, table_name=f"{OPS_SCHEMA}.migration_log")
mig_log.ensure_exists()

succeeded = 0
failed = 0

for r in rollback_rows:
    catalog, schema, name = r["catalog"], r["schema"], r["name"]
    obj_type = r["object_type"]
    table_type = r["table_type"]
    pre_fqn = r["pre_migration_fqn"]
    original_path = r["original_storage_path"]
    orig_fqn = quote_fqn(catalog, schema, name)

    # Build the rollback SQL plan based on what 03b did.
    steps: list[tuple[str, str]] = []

    if obj_type == "TABLE" and pre_fqn:
        # Managed table — undo the rename swap.
        steps.append(("drop migrated TABLE", f"DROP TABLE {orig_fqn}"))
        steps.append(("rename shadow back", f"ALTER TABLE {pre_fqn} RENAME TO {orig_fqn}"))
    elif obj_type == "TABLE" and table_type == "EXTERNAL":
        # External table — drop + recreate at the original path.
        if not original_path:
            print(f"  SKIP {catalog}.{schema}.{name}: external table but no original_storage_path in inventory")
            continue
        fmt = (r["data_source_format"] or "DELTA").upper()
        steps.append(("drop migrated TABLE", f"DROP TABLE {orig_fqn}"))
        steps.append(("recreate at old path",
                      f"CREATE EXTERNAL TABLE {orig_fqn} USING {fmt} LOCATION '{original_path}'"))
    elif obj_type == "VOLUME":
        # External volume — drop + recreate.
        if not original_path:
            print(f"  SKIP {catalog}.{schema}.{name}: volume but no original_storage_path")
            continue
        steps.append(("drop migrated VOLUME", f"DROP VOLUME {orig_fqn}"))
        steps.append(("recreate volume at old path",
                      f"CREATE EXTERNAL VOLUME {orig_fqn} LOCATION '{original_path}'"))
    else:
        print(f"  SKIP {catalog}.{schema}.{name}: cannot infer rollback plan "
              f"(object_type={obj_type}, table_type={table_type}, pre_fqn={pre_fqn})")
        continue

    if DRY_RUN:
        for label, sql in steps:
            print(f"  DRY [{label}]: {sql}")
        continue

    try:
        for label, sql in steps:
            spark.sql(sql)
        mig_log.update(catalog=catalog, schema=schema, name=name, status="rolled_back")
        succeeded += 1
        print(f"  rolled back: {catalog}.{schema}.{name}")
    except Exception as e:
        mig_log.update(catalog=catalog, schema=schema, name=name,
                       status="rollback_failed", error_trace=str(e))
        failed += 1
        print(f"  FAILED rollback for {catalog}.{schema}.{name}: {e}")

if not DRY_RUN:
    print(f"\nRollback complete: {succeeded} rolled back, {failed} failed.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Revert schema storage_root via REST PATCH

# COMMAND ----------
from utils.paths import classify_url

schemas_to_revert = []
seen = set()
inv_rows = spark.table(f"{OPS_SCHEMA}.inventory").collect()
for r in inv_rows:
    key = (r["catalog"], r["schema"])
    if key in seen:
        continue
    seen.add(key)
    parent = r["parent_managed_location"]
    # Use classify_url (not parsed.account == NEW) so prefix-mode works.
    # In prefix mode the bucket equals OLD's bucket portion too; classify_url
    # checks the full prefix path.
    if parent and classify_url(parent, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT) == "new":
        old_path = rewrite_account_in_path(parent, OLD_STORAGE_ACCOUNT,
                                           old_account=NEW_STORAGE_ACCOUNT)
        schemas_to_revert.append((r["catalog"], r["schema"], old_path))

print(f"Schemas to revert: {len(schemas_to_revert)}")
for catalog, sch, old_path in schemas_to_revert:
    if DRY_RUN:
        print(f"  DRY: PATCH /api/2.1/unity-catalog/schemas/{catalog}.{sch} "
              f"storage_root='{old_path}'")
    else:
        try:
            set_schema_storage_root(
                api_client=w.api_client,
                catalog=catalog, schema=sch, storage_root=old_path,
            )
            print(f"  reverted: {catalog}.{sch}")
        except Exception as e:
            print(f"  FAILED schema revert {catalog}.{sch}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — Revert catalog storage_root via REST PATCH

# COMMAND ----------
class _SdkRest:
    def __init__(self, w):
        self._api = w.api_client
    def get(self, path: str) -> dict:
        return self._api.do("GET", path)


client = UcClient(sdk=w, rest=_SdkRest(w))
catalogs = client.list_catalogs()
for c in catalogs:
    if not c.storage_root:
        continue
    if classify_url(c.storage_root, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT) == "new":
        old_path = rewrite_account_in_path(c.storage_root, OLD_STORAGE_ACCOUNT,
                                           old_account=NEW_STORAGE_ACCOUNT)
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
# MAGIC ## Step 6 — Verify
# MAGIC
# MAGIC Re-run `01_discovery`. Every previously-migrated object should now
# MAGIC classify as `consistent_old` again.
