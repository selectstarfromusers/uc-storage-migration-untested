# Databricks notebook source
# MAGIC %md
# MAGIC # 05_cleanup — Drop `__pre_migration` shadow tables
# MAGIC
# MAGIC **Purpose:** After `04_validation` reports `overall_pass=True` for
# MAGIC every migrated object and a grace period has elapsed, drop the
# MAGIC `__pre_migration` shadow tables left behind by `03b_forward_migrate`.
# MAGIC
# MAGIC **Inputs:** `<OPS_SCHEMA>.migration_log` — picks rows where
# MAGIC `status='validated'` and `pre_migration_fqn IS NOT NULL`.
# MAGIC
# MAGIC **Outputs:** `<OPS_SCHEMA>.cleanup_log` — one row per dropped
# MAGIC shadow (or per failed drop).
# MAGIC
# MAGIC **Side effects:** DESTRUCTIVE and IRREVERSIBLE. Dropping these
# MAGIC shadows removes both the UC table definitions and the underlying
# MAGIC Delta files at the OLD storage location. After cleanup,
# MAGIC `03a_rollback` can no longer restore the original state.
# MAGIC
# MAGIC **Gates:**
# MAGIC - `utils/config.py:POST_VALIDATION_CLEANUP_OK = True` — policy gate
# MAGIC - `DRY_RUN = False` in this notebook — per-run gate
# MAGIC
# MAGIC Both must be set for any DROP to execute. Either alone produces
# MAGIC only a preview.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Path setup

# COMMAND ----------
import os
import sys


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
# All values come from utils/config.py — edit there, not here.
from utils import config as _cfg
_cfg.resolve_config(spark=spark)
OPS_SCHEMA = _cfg.OPS_SCHEMA
POST_VALIDATION_CLEANUP_OK = _cfg.POST_VALIDATION_CLEANUP_OK

# Per-run operational gate — stays in this notebook so cleanup requires
# both the config policy bit AND an explicit non-DRY_RUN here.
DRY_RUN = True

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup + audit log table

# COMMAND ----------
# Create the cleanup_log table if it doesn't exist. VARIANT-friendly.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.cleanup_log (
    ts TIMESTAMP NOT NULL,
    pre_migration_fqn STRING NOT NULL,
    status STRING NOT NULL,    -- 'dropped' | 'failed' | 'dry'
    error_message STRING
) USING DELTA
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Find pre-migration shadows to drop

# COMMAND ----------
log_rows = spark.sql(
    f"SELECT pre_migration_fqn FROM {OPS_SCHEMA}.migration_log "
    f"WHERE status = 'validated' AND pre_migration_fqn IS NOT NULL"
).collect()

shadows = [row["pre_migration_fqn"] for row in log_rows]
print(f"Found {len(shadows)} pre_migration shadow(s) eligible for cleanup.")
if not shadows:
    print("Nothing to do. Exiting.")
    dbutils.notebook.exit("no shadows")  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gate check

# COMMAND ----------
if not POST_VALIDATION_CLEANUP_OK:
    print(
        "POST_VALIDATION_CLEANUP_OK = False in utils/config.py. "
        "Set it to True to permit cleanup. Showing plan only — no DROPs will run.\n"
    )
    DRY_RUN = True   # force preview even if customer set False here

if DRY_RUN:
    print("DRY_RUN = True — showing plan, no DROPs will run:\n")
    for fqn in shadows[:50]:
        print(f"  WOULD DROP {fqn}")
    if len(shadows) > 50:
        print(f"  ... and {len(shadows) - 50} more")
    print(
        f"\nTo apply: set POST_VALIDATION_CLEANUP_OK=True in utils/config.py "
        f"AND DRY_RUN=False in this notebook, then re-run."
    )
    dbutils.notebook.exit("dry run")  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ## ⚠️ Apply (irreversible)
# MAGIC
# MAGIC Dropping shadows removes both the UC table definition AND the
# MAGIC underlying Delta files at OLD. After this completes, the original
# MAGIC state cannot be recovered via `03a_rollback`.

# COMMAND ----------
print(f"⚠️  About to DROP {len(shadows)} __pre_migration shadow table(s).")
print("⚠️  After this completes, 03a_rollback can NO LONGER restore the original state.")
print()

success = 0
failed = 0
for fqn in shadows:
    try:
        spark.sql(f"DROP TABLE {fqn}")
        spark.sql(
            f"INSERT INTO {OPS_SCHEMA}.cleanup_log VALUES "
            f"(current_timestamp(), '{fqn}', 'dropped', NULL)"
        )
        print(f"  dropped {fqn}")
        success += 1
    except Exception as e:
        msg = str(e).replace("'", "''")[:500]
        spark.sql(
            f"INSERT INTO {OPS_SCHEMA}.cleanup_log VALUES "
            f"(current_timestamp(), '{fqn}', 'failed', '{msg}')"
        )
        print(f"  FAILED to drop {fqn}: {e}")
        failed += 1

print(f"\nCleanup complete: {success} dropped, {failed} failed.")
if failed:
    print(f"See {OPS_SCHEMA}.cleanup_log for failure details.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC The migration is now final. If you also want to retire the OLD
# MAGIC storage credential / external location / bucket itself, do that
# MAGIC manually via the Databricks UI or CLI after confirming no other
# MAGIC workloads depend on them.
