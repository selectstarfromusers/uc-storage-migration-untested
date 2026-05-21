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
        # Returns like "/Users/.../uc-storage-migration/notebooks/01_discovery"
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
from utils import config as _cfg
_cfg.resolve_config(spark=spark)  # auto-derive REPOINT_CATALOG, SCHEMAS_TO_REPOINT if unset
_cfg.validate_config_for_repoint()  # raises with clear message if anything missing
CATALOG = _cfg.REPOINT_CATALOG
SCHEMAS_TO_REPOINT = _cfg.SCHEMAS_TO_REPOINT
NEW_STORAGE_PREFIX = _cfg.NEW_STORAGE_PREFIX

# Per-run operational gate — stays in this notebook so a single edit to
# utils/config.py can't arm a destructive op across multiple notebooks.
CONFIRMED = False                   # set True to actually repoint

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------
from databricks.sdk import WorkspaceClient

from utils.uc_admin import set_schema_storage_root, get_schema_storage_root


w = WorkspaceClient()
# validate_config_for_repoint above already ensures SCHEMAS_TO_REPOINT
# is non-empty.

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
