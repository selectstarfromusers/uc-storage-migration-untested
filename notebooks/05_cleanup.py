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
# All values come from utils/config.py — edit there, not here.
import importlib
from utils import config as _cfg
importlib.reload(_cfg)  # pick up edits to utils/config.py without restarting Python
_cfg.resolve_config(spark=spark)
OPS_SCHEMA = _cfg.OPS_SCHEMA
POST_VALIDATION_CLEANUP_OK = _cfg.POST_VALIDATION_CLEANUP_OK

# FAIL CLOSED: cleanup is destructive, so refuse to proceed on any
# unloaded/invalid config rather than risk building a wrong target (e.g.
# "None.migration_log"). A bad OPS_SCHEMA must abort, never guess.
if not OPS_SCHEMA or not isinstance(OPS_SCHEMA, str) or OPS_SCHEMA.count(".") != 1 or "None" in OPS_SCHEMA:
    raise RuntimeError(
        f"OPS_SCHEMA is not a valid 'catalog.schema' value ({OPS_SCHEMA!r}). "
        "Refusing to run cleanup. Set CATALOG_ALLOWLIST/OPS_SCHEMA in utils/config.py."
    )
if not isinstance(POST_VALIDATION_CLEANUP_OK, bool):
    raise RuntimeError(
        f"POST_VALIDATION_CLEANUP_OK must be a bool, got {POST_VALIDATION_CLEANUP_OK!r}."
    )

# Per-run operational gate — stays in this notebook so cleanup requires
# both the config policy bit AND an explicit non-DRY_RUN here.
DRY_RUN = True

# Emergency recovery gate (Section: Emergency UNDROP). Leave False for normal
# cleanup runs; set True ONLY to restore shadows this notebook just dropped.
EMERGENCY_UNDROP = False

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup + audit log table

# COMMAND ----------
# Create the cleanup_log table if it doesn't exist.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.cleanup_log (
    ts TIMESTAMP NOT NULL,
    pre_migration_fqn STRING NOT NULL,
    object_type STRING,
    status STRING NOT NULL,    -- 'dropped' | 'failed' | 'refused_no_live' | 'skipped_absent' | 'undropped' | 'undrop_failed'
    error_message STRING
) USING DELTA
""")
# Forward-compat: add object_type if cleanup_log predates this column.
try:
    spark.sql(f"ALTER TABLE {OPS_SCHEMA}.cleanup_log ADD COLUMN IF NOT EXISTS object_type STRING")
except Exception:
    pass

from utils.cleanup import (
    validate_cleanup_targets, build_drop_sql, build_undrop_sql, can_undrop,
)
from utils.sql import quote_fqn


def _object_exists(fqn_quoted: str, object_type: str) -> bool:
    """True if the table/volume exists (information_schema)."""
    parts = [p for p in fqn_quoted.replace("`", "").split(".")]
    if len(parts) != 3:
        return False
    cat, sch, name = parts
    view, col = ("volumes", "volume") if (object_type or "").upper() == "VOLUME" else ("tables", "table")
    try:
        n = spark.sql(
            f"SELECT count(*) AS n FROM system.information_schema.{view} "
            f"WHERE {col}_catalog = '{cat}' AND {col}_schema = '{sch}' AND {col}_name = '{name}'"
        ).collect()[0]["n"]
        return int(n) > 0
    except Exception:
        return False


def _log_cleanup(fqn, otype, status, error_message=None):
    em = "NULL" if error_message is None else "'" + str(error_message).replace("'", "''")[:500] + "'"
    spark.sql(
        f"INSERT INTO {OPS_SCHEMA}.cleanup_log "
        f"(ts, pre_migration_fqn, object_type, status, error_message) VALUES "
        f"(current_timestamp(), '{fqn}', '{otype}', '{status}', {em})"
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Find pre-migration shadows to drop

# COMMAND ----------
log_rows = spark.sql(
    f"SELECT pre_migration_fqn, object_type FROM {OPS_SCHEMA}.migration_log "
    f"WHERE status = 'validated' AND pre_migration_fqn IS NOT NULL"
).collect()

raw_targets = [(row["pre_migration_fqn"], row["object_type"]) for row in log_rows]
print(f"Found {len(raw_targets)} validated row(s) with a pre_migration shadow.")

# SAFETY GATE — only genuine `__pre_migration` shadows may ever be dropped.
# If ANY target fails the convention (wrong suffix, not 3-part, OPS_SCHEMA,
# bad type), we ABORT and drop NOTHING — a malformed/edited migration_log
# must never let cleanup touch a live object.
accepted, rejected = validate_cleanup_targets(raw_targets, ops_schema=OPS_SCHEMA)
if rejected:
    print(f"\n✗ {len(rejected)} target(s) FAILED the __pre_migration safety check:")
    for fqn, reason in rejected[:50]:
        print(f"  REJECT {fqn}: {reason}")
    raise RuntimeError(
        f"Refusing to run cleanup: {len(rejected)} target(s) are not valid "
        "__pre_migration shadows (possible data-loss risk). Investigate "
        f"{OPS_SCHEMA}.migration_log before retrying. NOTHING was dropped."
    )

if not accepted:
    print("No valid shadows to drop. Exiting.")
    dbutils.notebook.exit("no shadows")  # noqa: F821
print(f"All {len(accepted)} target(s) passed the safety check.")

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
    for t in accepted[:50]:
        print(f"  WOULD DROP {t['object_type']} {t['fqn']}  (keeps live {t['live_fqn']})")
    if len(accepted) > 50:
        print(f"  ... and {len(accepted) - 50} more")
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
print(f"⚠️  About to DROP {len(accepted)} __pre_migration shadow object(s).")
print("⚠️  After this completes, 03a_rollback can NO LONGER restore the original state.")
print("⚠️  TABLE shadows are recoverable via the Emergency UNDROP section (within the")
print("    UC retention window); VOLUME shadows have NO UNDROP — their drop is final.")
print()

success = failed = refused = skipped = 0
for t in accepted:
    fqn, otype, live_fqn = t["fqn"], t["object_type"], t["live_fqn"]
    # Guard 1: the shadow must actually exist (else nothing to do — idempotent).
    if not _object_exists(fqn, otype):
        print(f"  skip (already gone) {fqn}")
        _log_cleanup(fqn, otype, "skipped_absent")
        skipped += 1
        continue
    # Guard 2: the LIVE migrated object must exist — never drop the shadow if
    # it could be the only surviving copy.
    if not live_fqn or not _object_exists(live_fqn, otype):
        print(f"  REFUSE {fqn}: live object {live_fqn} not found — would risk the only copy")
        _log_cleanup(fqn, otype, "refused_no_live", f"live {live_fqn} missing")
        refused += 1
        continue
    try:
        spark.sql(build_drop_sql(fqn, otype))
        _log_cleanup(fqn, otype, "dropped")
        print(f"  dropped {otype} {fqn}")
        success += 1
    except Exception as e:
        _log_cleanup(fqn, otype, "failed", str(e))
        print(f"  FAILED to drop {fqn}: {e}")
        failed += 1

print(f"\nCleanup complete: {success} dropped, {refused} refused (no live copy), "
      f"{skipped} already-absent, {failed} failed.")
if refused:
    print("⚠️  Refused drops mean a migrated object is MISSING — investigate before retrying.")
if failed:
    print(f"See {OPS_SCHEMA}.cleanup_log for failure details.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🚑 Emergency UNDROP — restore shadows this notebook just dropped
# MAGIC
# MAGIC Use ONLY if you dropped shadows and need them back. `UNDROP TABLE`
# MAGIC restores a managed table within the UC retention window (default 7 days,
# MAGIC and only if its files haven't been vacuumed/GC'd). **Volumes have no
# MAGIC UNDROP** — a dropped volume shadow can't be restored this way.
# MAGIC
# MAGIC To arm: set `EMERGENCY_UNDROP = True` in the Config cell, re-run it, then
# MAGIC run this cell. After restoring the shadows, run **`03a_rollback`** to
# MAGIC rename them back to their live names (UNDROP only brings the shadow back;
# MAGIC rollback completes the restore).
# MAGIC
# MAGIC This cell only needs the Config + Setup cells to have run — it reads what
# MAGIC to restore from `cleanup_log`, so it survives a session reset.

# COMMAND ----------
if not EMERGENCY_UNDROP:
    print("EMERGENCY_UNDROP = False — skipping recovery. "
          "Set it True in the Config cell only to restore dropped shadows.")
else:
    undrop_rows = spark.sql(
        f"SELECT pre_migration_fqn, object_type FROM {OPS_SCHEMA}.cleanup_log "
        f"WHERE status = 'dropped'"
    ).collect()
    print(f"🚑 Attempting to UNDROP {len(undrop_rows)} previously-dropped shadow(s)...\n")
    restored = vol_blocked = und_failed = 0
    for r in undrop_rows:
        fqn = r["pre_migration_fqn"]
        otype = (r["object_type"] or "TABLE")
        parts = [p for p in fqn.replace("`", "").split(".")]
        fqn_q = quote_fqn(*parts) if len(parts) == 3 else fqn
        if not can_undrop(otype):
            print(f"  CANNOT UNDROP {otype} {fqn} — volumes have no UNDROP; restore from an external backup.")
            _log_cleanup(fqn, otype, "undrop_failed", "volumes cannot be undropped")
            vol_blocked += 1
            continue
        try:
            spark.sql(build_undrop_sql(fqn_q))
            _log_cleanup(fqn, otype, "undropped")
            print(f"  undropped {fqn}")
            restored += 1
        except Exception as e:
            _log_cleanup(fqn, otype, "undrop_failed", str(e))
            print(f"  FAILED to undrop {fqn}: {e}")
            und_failed += 1
    print(f"\nUNDROP: {restored} restored, {vol_blocked} volume(s) (no UNDROP), {und_failed} failed.")
    if restored:
        print("➡️  Next: run 03a_rollback to rename the restored shadows back to their live names.")
    if vol_blocked or und_failed:
        print("⚠️  Some shadows could NOT be restored here — restore from an external backup.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC The migration is now final. If you also want to retire the OLD
# MAGIC storage credential / external location / bucket itself, do that
# MAGIC manually via the Databricks UI or CLI after confirming no other
# MAGIC workloads depend on them.
