# Databricks notebook source
# MAGIC %md
# MAGIC # 00_repoint_schemas — Set schema storage_root to NEW prefix
# MAGIC
# MAGIC **Purpose:** Before running `01_discovery` on a native UC catalog,
# MAGIC each schema's `storage_root` must point at the NEW location. SQL
# MAGIC `ALTER SCHEMA SET MANAGED LOCATION` is rejected for native UC
# MAGIC (`UC_COMMAND_NOT_SUPPORTED.NON_HMS_FEDERATED_ENTITY`). The
# MAGIC underlying UC REST API allows this — use `w.api_client.do(PATCH, ...)`.
# MAGIC
# MAGIC **Inputs:** Hardcoded config — `CATALOG`, `SCHEMAS_TO_REPOINT`,
# MAGIC `NEW_STORAGE_PREFIX`.
# MAGIC
# MAGIC **Outputs:** Each schema's `storage_root` set to
# MAGIC `<NEW_STORAGE_PREFIX>/<schema>/`. Existing tables stay at their
# MAGIC current physical paths (UC does not auto-move them); only new
# MAGIC tables go to the new location. After this runs, `01_discovery`
# MAGIC will classify existing tables as `drift_managed_on_old`.
# MAGIC
# MAGIC **Side effects:** UC metadata change only. No data movement.
# MAGIC
# MAGIC **Required:** `CONFIRMED = True`. Default is False — running
# MAGIC without confirmation prints the plan only.

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
CATALOG = "your_catalog"           # e.g. "artm_dev_catalog"
# Schemas to repoint. Skip system schemas (information_schema) and any
# schema you don't want to migrate (typically the one used for migration
# operations / logging, if any).
SCHEMAS_TO_REPOINT: list[str] = []  # e.g. ["bronze", "silver", "gold"]
# NEW prefix — each schema's storage_root will be set to
# f"{NEW_STORAGE_PREFIX}/{schema}".
NEW_STORAGE_PREFIX = "abfss://container@newacct.dfs.core.windows.net/path"
CONFIRMED = False                   # set True to actually repoint

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------
from databricks.sdk import WorkspaceClient

from utils.uc_admin import set_schema_storage_root, get_schema_storage_root


w = WorkspaceClient()

if not SCHEMAS_TO_REPOINT:
    raise ValueError("SCHEMAS_TO_REPOINT is empty — set the schema list in the config cell.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Plan

# COMMAND ----------
plan = []
for schema in SCHEMAS_TO_REPOINT:
    current = get_schema_storage_root(api_client=w.api_client, catalog=CATALOG, schema=schema)
    target = f"{NEW_STORAGE_PREFIX.rstrip('/')}/{schema}"
    plan.append((schema, current, target))
    print(f"  {CATALOG}.{schema}")
    print(f"    current: {current or '(inherits catalog default)'}")
    print(f"    new:     {target}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Execute (gated by CONFIRMED)

# COMMAND ----------
if not CONFIRMED:
    print("CONFIRMED = False — plan only. Set CONFIRMED = True to apply.")
else:
    for schema, current, target in plan:
        try:
            result = set_schema_storage_root(
                api_client=w.api_client,
                catalog=CATALOG,
                schema=schema,
                storage_root=target,
            )
            print(f"  OK {CATALOG}.{schema}: storage_root -> {result.get('storage_root')}")
        except Exception as e:
            print(f"  FAILED {CATALOG}.{schema}: {type(e).__name__}: {e}")
            raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC 1. Run `01_discovery` — existing tables will classify as
# MAGIC    `drift_managed_on_old` because their `storage_path` is on the
# MAGIC    old prefix while the schema's `parent_managed_location` is now
# MAGIC    on the new prefix.
# MAGIC 2. Run `02_decision_report` to confirm the recommendation.
# MAGIC 3. Run `03b_forward_migrate` to migrate. Managed Delta uses
# MAGIC    DEEP CLONE which physically moves data via the new schema
# MAGIC    storage_root. External tables require the data to already be
# MAGIC    at the new path (e.g., via azcopy/rsync) before this step.
# MAGIC 4. Run `04_validation` to confirm every migrated object.
