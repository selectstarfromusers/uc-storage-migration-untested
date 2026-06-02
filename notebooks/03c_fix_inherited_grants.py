# Databricks notebook source
# MAGIC %md
# MAGIC # 03c_fix_inherited_grants — Remove wrongly-materialized inherited grants
# MAGIC
# MAGIC **Purpose:** Earlier `03b` runs captured grants with `SHOW GRANTS ON TABLE`,
# MAGIC which returns the *effective* set — including privileges **inherited** from
# MAGIC the parent catalog/schema. The replay re-applied every captured grant
# MAGIC `ON TABLE`, so inherited privileges got materialized as **explicit
# MAGIC table-level grants** on the migrated objects, defeating UC inheritance.
# MAGIC (Fixed going forward in `utils/governance.filter_direct_grants`.)
# MAGIC
# MAGIC This notebook reconciles already-migrated objects: for each object it
# MAGIC computes the grants that were inherited (per the stored snapshot) yet are
# MAGIC now present as explicit object-level grants, and `REVOKE`s exactly those —
# MAGIC leaving genuinely-direct grants untouched.
# MAGIC
# MAGIC **Inputs:** `<OPS_SCHEMA>.object_metadata_snapshot` (the per-object capture
# MAGIC written by `03b`), plus a live `SHOW GRANTS` on each object (cross-check).
# MAGIC
# MAGIC **Outputs:** Prints a per-object plan. With `CONFIRMED=True` + `DRY_RUN=False`,
# MAGIC executes the `REVOKE`s. Writes nothing to state tables.
# MAGIC
# MAGIC **Side effects:** With gates armed, DESTRUCTIVE to ACLs (revokes explicit
# MAGIC table/volume grants). It only revokes grants that are BOTH (a) inherited in
# MAGIC the snapshot and (b) currently present as explicit object-level grants, and
# MAGIC never revokes a privilege that was also a genuine direct grant. Access is
# MAGIC preserved because the revoked privileges still apply via catalog/schema
# MAGIC inheritance.
# MAGIC
# MAGIC **Required to apply:** `CONFIRMED = True`. Default `DRY_RUN = True`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Path setup

# COMMAND ----------
import os
import sys


def _notebook_path() -> str | None:
    """Return the absolute workspace path of this notebook, or None."""
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
        p = ctx.notebookPath().get()
        return f"/Workspace{p}" if p and not p.startswith("/Workspace") else p
    except Exception:
        return None


def _add_utils_to_path() -> bool:
    """Walk up looking for sibling utils/. Returns True if found."""
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
        "above this block (then set REPO_ROOT_HINT in utils/config.py)."
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config + gates

# COMMAND ----------
import importlib
from utils import config as _cfg
importlib.reload(_cfg)
_cfg.resolve_config(spark=spark)
OPS_SCHEMA = _cfg.OPS_SCHEMA

# Per-run operational gates. Default is a safe, read-only preview.
CONFIRMED = False   # must be True to execute any REVOKE
DRY_RUN = True      # when True, only prints the plan

SNAPSHOT_TABLE = f"{OPS_SCHEMA}.object_metadata_snapshot"
print(f"OPS_SCHEMA          = {OPS_SCHEMA}")
print(f"snapshot table      = {SNAPSHOT_TABLE}")
print(f"CONFIRMED / DRY_RUN = {CONFIRMED} / {DRY_RUN}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build the remediation plan
# MAGIC
# MAGIC For each object in the snapshot:
# MAGIC 1. `inherited` = snapshot grants whose `object_type` ∈ {CATALOG, SCHEMA, METASTORE}.
# MAGIC 2. `direct`    = snapshot grants whose `object_type` is the securable itself (TABLE/VOLUME).
# MAGIC 3. `live_direct` = `(principal, privilege)` pairs **currently** granted directly on the object.
# MAGIC 4. **revoke** = `(inherited − direct) ∩ live_direct` — i.e. privileges that were only
# MAGIC    inherited yet are now explicit on the object. Genuine direct grants are never touched.

# COMMAND ----------
import json

from utils.sql import quote_ident, quote_fqn
from utils.governance import (
    build_show_grants_sql, parse_show_grants_rows, filter_direct_grants,
)

_INHERITED = {"CATALOG", "SCHEMA", "METASTORE"}


def _securable_kw(object_type: str) -> str:
    return "VOLUME" if (object_type or "").upper() == "VOLUME" else "TABLE"


def _live_direct_pairs(spark, *, catalog, schema, name, object_type) -> set:
    """(principal, privilege) pairs granted DIRECTLY on the live object now."""
    try:
        rows = [r.asDict() for r in spark.sql(build_show_grants_sql(
            catalog=catalog, schema=schema, name=name, object_type=object_type,
        )).collect()]
    except Exception:
        return set()
    direct = filter_direct_grants(parse_show_grants_rows(rows), object_type=object_type)
    return {(g.principal, g.privilege) for g in direct}


def _plan_for_object(spark, *, catalog, schema, name, object_type, snapshot_json) -> list[dict]:
    try:
        grants = json.loads(snapshot_json).get("grants", [])
    except Exception:
        return []
    kw = _securable_kw(object_type)
    direct = {(g["principal"], g["privilege"]) for g in grants
              if (g.get("object_type") or "").upper() == kw}
    inherited = {(g["principal"], g["privilege"]) for g in grants
                 if (g.get("object_type") or "").upper() in _INHERITED}
    # Only the privileges that were inherited-only (not also direct in the source).
    candidates = inherited - direct
    if not candidates:
        return []
    live_direct = _live_direct_pairs(
        spark, catalog=catalog, schema=schema, name=name, object_type=object_type)
    fqn = quote_fqn(catalog, schema, name)
    out = []
    for principal, privilege in sorted(candidates):
        if (principal, privilege) not in live_direct:
            continue  # not currently materialized at object level — nothing to undo
        out.append({
            "fqn": f"{catalog}.{schema}.{name}",
            "object_type": kw,
            "principal": principal,
            "privilege": privilege,
            "sql": f"REVOKE {privilege} ON {kw} {fqn} FROM {quote_ident(principal)}",
        })
    return out


snap_rows = spark.sql(
    f"SELECT catalog, schema, name, object_type, snapshot_json FROM {SNAPSHOT_TABLE}"
).collect()
print(f"Snapshot rows: {len(snap_rows)}")

plan: list[dict] = []
for r in snap_rows:
    plan.extend(_plan_for_object(
        spark, catalog=r["catalog"], schema=r["schema"], name=r["name"],
        object_type=r["object_type"], snapshot_json=r["snapshot_json"],
    ))

objects_affected = sorted({p["fqn"] for p in plan})
print(f"\nREVOKEs planned: {len(plan)} across {len(objects_affected)} object(s)\n")
for fqn in objects_affected:
    items = [p for p in plan if p["fqn"] == fqn]
    print(f"  {fqn}  ({len(items)} revoke(s))")
    for p in items:
        print(f"      - {p['privilege']:<16} FROM {p['principal']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Preview the exact SQL (always safe)

# COMMAND ----------
for p in plan:
    print(p["sql"])
if not plan:
    print("Nothing to remediate — no inherited grants were materialized at object level.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Apply — gated
# MAGIC Set `CONFIRMED = True` **and** `DRY_RUN = False` in the Config cell, re-run, then run this cell.

# COMMAND ----------
if not plan:
    print("No-op: empty plan.")
elif DRY_RUN or not CONFIRMED:
    print(f"[DRY RUN] Would execute {len(plan)} REVOKE(s). "
          f"Set CONFIRMED=True and DRY_RUN=False to apply.")
else:
    applied, failed = 0, []
    for p in plan:
        try:
            spark.sql(p["sql"])
            applied += 1
        except Exception as e:
            failed.append((p["sql"], str(e)))
    print(f"Applied {applied} REVOKE(s); {len(failed)} failed.")
    for sql, err in failed:
        print(f"  FAILED: {sql}\n          {err}")
