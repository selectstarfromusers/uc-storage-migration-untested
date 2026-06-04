# Databricks notebook source
# MAGIC %md
# MAGIC # 01_discovery — UC storage inventory
# MAGIC
# MAGIC **Purpose:** Build a comprehensive inventory of every UC object (tables, volumes,
# MAGIC registered models, external locations, metastore root) and classify each by
# MAGIC which storage account it actually references.
# MAGIC
# MAGIC **Inputs:** UC catalogs (filtered by `CATALOG_ALLOWLIST`).
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `<OPS_SCHEMA>.inventory` — one row per UC object with classification
# MAGIC - `<OPS_SCHEMA>.external_locations` — registered external locations
# MAGIC - `<OPS_SCHEMA>.lineage_consumers` — downstream consumers of in-scope objects
# MAGIC - Markdown summary cell at the end
# MAGIC
# MAGIC **Side effects:** Read-only. Writes only to `<OPS_SCHEMA>` Delta tables. No
# MAGIC modification to in-scope catalogs/schemas/tables.
# MAGIC
# MAGIC **Re-run:** Safe to re-run; `inventory` is fully overwritten each run.
# MAGIC
# MAGIC **Workspace layout requirement:** The `utils/` directory must live in the
# MAGIC same parent folder as this notebook (i.e., `<parent>/notebooks/01_discovery`
# MAGIC and `<parent>/utils/*.py`). The setup cell below adds `<parent>` to
# MAGIC `sys.path` automatically. If your workspace structure differs, set
# MAGIC `REPO_ROOT_HINT` in `utils/config.py`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Path setup — make `utils/` importable

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
_cfg.resolve_config(spark=spark)  # auto-derive OPS_SCHEMA from CATALOG_ALLOWLIST[0] if unset
_cfg.validate_config_for_discovery()  # raises if CATALOG_ALLOWLIST empty + ALLOW_ALL_CATALOGS not set
OLD_STORAGE_ACCOUNT = _cfg.OLD_STORAGE_ACCOUNT
NEW_STORAGE_ACCOUNT = _cfg.NEW_STORAGE_ACCOUNT
CATALOG_ALLOWLIST = _cfg.CATALOG_ALLOWLIST
OPS_SCHEMA = _cfg.OPS_SCHEMA
COLLECT_SIZES = _cfg.COLLECT_SIZES
LINEAGE_LOOKBACK_DAYS = _cfg.LINEAGE_LOOKBACK_DAYS

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
)

from utils.uc_client import UcClient
from utils.discovery import ObjectRecord, classify_object, _requires_pipeline_handling
from utils.state import InventoryWriter
from utils.lineage import build_lineage_consumers_query
from utils.reporting import (
    DecisionThresholds, compute_recommendation, render_summary_markdown,
)
from utils.sql import quote_fqn
from utils.storage_path import resolve_storage_path


class _SdkRest:
    """Wrap WorkspaceClient.api_client for the UcClient REST protocol."""
    def __init__(self, w: WorkspaceClient):
        self._api = w.api_client

    def get(self, path: str) -> dict:
        return self._api.do("GET", path)


w = WorkspaceClient()
client = UcClient(sdk=w, rest=_SdkRest(w))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Metastore + external locations

# COMMAND ----------
# summary() works for any user with USE METASTORE; .get() requires metastore admin
# (most customer SAs aren't admins — use summary() to keep discovery accessible).
metastore = w.metastores.summary()
print(f"Metastore: {metastore.name} ({metastore.metastore_id})")
print(f"  storage_root: {metastore.storage_root}")
print(f"  region: {metastore.region}")

ext_locs = client.list_external_locations()
print(f"\nExternal locations: {len(ext_locs)}")
for el in ext_locs:
    print(f"  {el.name} -> {el.url} (cred={el.credential_name}, region={el.region}, read_only={el.read_only})")

# Use an explicit Spark schema so an empty ext_locs list doesn't produce a schemaless DataFrame.
# `region` is always NULL — UC's external-locations API doesn't return it.
# isolation_mode + accessible_in_current_workspace are pulled through for diagnostics.
_EXT_LOC_SCHEMA = StructType([
    StructField("name", StringType(), False),
    StructField("url", StringType(), False),
    StructField("credential_name", StringType(), False),
    StructField("read_only", BooleanType(), False),
    StructField("region", StringType(), True),
    StructField("isolation_mode", StringType(), True),
    StructField("accessible_in_current_workspace", BooleanType(), True),
])
ext_rows = [
    (el.name, el.url, el.credential_name, el.read_only, el.region,
     el.isolation_mode, el.accessible_in_current_workspace)
    for el in ext_locs
]
ext_df = spark.createDataFrame(ext_rows, schema=_EXT_LOC_SCHEMA)
ext_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{OPS_SCHEMA}.external_locations")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Enumerate catalogs and schemas

# COMMAND ----------
catalogs = client.list_catalogs(allowlist=CATALOG_ALLOWLIST or None)
print(f"In-scope catalogs: {len(catalogs)}")
for c in catalogs:
    print(f"  {c.name} (type={c.catalog_type}, storage_root={c.storage_root})")

schemas_by_catalog = {}
for c in catalogs:
    if c.catalog_type in {"FOREIGN_CATALOG", "DELTASHARING_CATALOG", "SYSTEM_CATALOG"}:
        continue
    schemas_by_catalog[c.name] = client.list_schemas(c.name)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Enumerate tables, volumes, tags, grants from information_schema

# COMMAND ----------
catalog_filter = (
    "(" + ", ".join(f"'{c.name}'" for c in catalogs) + ")"
    if CATALOG_ALLOWLIST else ""
)
where_clause = f"WHERE table_catalog IN {catalog_filter}" if catalog_filter else ""

tables_sql = f"""
SELECT
  table_catalog, table_schema, table_name,
  table_type, data_source_format,
  table_owner AS owner,
  created, last_altered,
  storage_path
FROM system.information_schema.tables
{where_clause}
"""
tables_df = spark.sql(tables_sql).toPandas()
print(f"Tables: {len(tables_df)}")

volumes_where = where_clause.replace("table_catalog", "volume_catalog")
volumes_sql = f"""
SELECT
  volume_catalog AS table_catalog,
  volume_schema AS table_schema,
  volume_name AS table_name,
  volume_type AS table_type,
  NULL AS data_source_format,
  volume_owner AS owner,
  created, last_altered,
  storage_location AS storage_path
FROM system.information_schema.volumes
{volumes_where}
"""
volumes_df = spark.sql(volumes_sql).toPandas()
print(f"Volumes: {len(volumes_df)}")

# Tag and grant counts per object (bulk pulls; tolerate view absence)
def _count_per_fqn(view: str, group_cols: tuple[str, str, str]) -> dict[tuple, int]:
    try:
        df = spark.sql(
            f"SELECT {group_cols[0]} AS c, {group_cols[1]} AS s, {group_cols[2]} AS n, count(*) AS k "
            f"FROM {view} {where_clause.replace('table_catalog', group_cols[0]) if where_clause else ''} "
            f"GROUP BY {group_cols[0]}, {group_cols[1]}, {group_cols[2]}"
        ).toPandas()
        return {(r["c"], r["s"], r["n"]): int(r["k"]) for _, r in df.iterrows()}
    except Exception as e:
        print(f"  (skipped {view}: {e})")
        return {}

tag_counts = _count_per_fqn(
    "system.information_schema.table_tags",
    ("catalog_name", "schema_name", "table_name"),
)
print(f"Tagged tables: {len(tag_counts)}")

grant_counts = _count_per_fqn(
    "system.information_schema.table_privileges",
    ("table_catalog", "table_schema", "table_name"),
)
print(f"Tables with grants: {len(grant_counts)}")

# Registered models — surfaced as inventory rows with classification
# 'requires_external_handling'. The repo doesn't migrate them (they're
# catalog-scoped metadata; their physical model files follow UC's own
# rules), but the customer needs to see them.
models_where = where_clause.replace("table_catalog", "catalog_name")
try:
    models_df = spark.sql(f"""
SELECT
  catalog_name AS table_catalog,
  schema_name AS table_schema,
  model_name AS table_name,
  'REGISTERED_MODEL' AS table_type,
  NULL AS data_source_format,
  model_owner AS owner,
  created, last_altered,
  NULL AS storage_path
FROM system.information_schema.models
{models_where}
""").toPandas()
    print(f"Registered models: {len(models_df)}")
except Exception as e:
    print(f"  (skipped models — system.information_schema.models unavailable: {e})")
    import pandas as pd
    models_df = pd.DataFrame(columns=[
        "table_catalog","table_schema","table_name","table_type",
        "data_source_format","owner","created","last_altered","storage_path"
    ])

# Functions / UDFs — same treatment. UC stores their definitions in the
# catalog; they follow whatever catalog they live in.
routines_where = where_clause.replace("table_catalog", "routine_catalog")
try:
    functions_df = spark.sql(f"""
SELECT
  routine_catalog AS table_catalog,
  routine_schema AS table_schema,
  routine_name AS table_name,
  'FUNCTION' AS table_type,
  NULL AS data_source_format,
  routine_owner AS owner,
  created, last_altered,
  NULL AS storage_path
FROM system.information_schema.routines
{routines_where}
""").toPandas()
    print(f"Functions / UDFs: {len(functions_df)}")
except Exception as e:
    print(f"  (skipped functions — system.information_schema.routines unavailable: {e})")
    import pandas as pd
    functions_df = pd.DataFrame(columns=[
        "table_catalog","table_schema","table_name","table_type",
        "data_source_format","owner","created","last_altered","storage_path"
    ])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Build ObjectRecords and classify

# COMMAND ----------
schema_locs = {
    (cat, s.name): s.storage_root
    for cat, schemas in schemas_by_catalog.items()
    for s in schemas
}
catalog_locs = {c.name: c.storage_root for c in catalogs}


def parent_managed_location(catalog: str, schema: str) -> str | None:
    return schema_locs.get((catalog, schema)) or catalog_locs.get(catalog)


# Visibility for size collection — populated as the loop runs.
_size_skipped: list[tuple[str, str, str]] = []  # (fqn, fmt-or-null, reason)


VOLUME_SIZE_MAX_FILES = 10000          # cap files walked per volume
VOLUME_SIZE_MAX_SECONDS_PER_VOLUME = 30  # cap wall time per volume


def _collect_volume_size(
    catalog: str, schema: str, name: str, fqn_display: str,
) -> int | None:
    """Recursively sum file sizes under a volume by walking `/Volumes/{cat}/{sch}/{vol}/`.

    Uses the UC-friendly /Volumes/... path, not the raw s3:// storage_path —
    dbutils.fs.ls on the raw __unitystorage S3 path doesn't work even though
    UC owns the credential. The /Volumes/ path goes through UC's vended-creds
    layer and lists correctly for any user with READ VOLUME.

    Bounded by VOLUME_SIZE_MAX_FILES and VOLUME_SIZE_MAX_SECONDS_PER_VOLUME so
    a pathological volume can't stall discovery. Failures logged to
    `_size_skipped` for visibility at the end of the run.
    """
    if not COLLECT_SIZES:
        return None
    path = f"/Volumes/{catalog}/{schema}/{name}/"
    import time
    deadline = time.monotonic() + VOLUME_SIZE_MAX_SECONDS_PER_VOLUME
    total = 0
    files_seen = 0
    stack = [path]
    try:
        while stack:
            if time.monotonic() > deadline:
                _size_skipped.append((fqn_display, "(volume)",
                                       f"walk exceeded {VOLUME_SIZE_MAX_SECONDS_PER_VOLUME}s budget"))
                return None
            cur = stack.pop()
            try:
                entries = dbutils.fs.ls(cur)  # noqa: F821 (Databricks builtin)
            except Exception as e:
                # path doesn't exist or permission denied — log once, skip
                _size_skipped.append((fqn_display, "(volume)",
                                       f"ls({cur[:60]}...): {type(e).__name__}: {str(e)[:80]}"))
                return None
            for f in entries:
                if files_seen >= VOLUME_SIZE_MAX_FILES:
                    _size_skipped.append((fqn_display, "(volume)",
                                           f"walk exceeded {VOLUME_SIZE_MAX_FILES} files"))
                    return None
                files_seen += 1
                if f.isDir():
                    stack.append(f.path)
                else:
                    total += int(f.size or 0)
        return total
    except Exception as e:
        _size_skipped.append((fqn_display, "(volume)",
                               f"{type(e).__name__}: {str(e)[:120]}"))
        return None


def _collect_size(catalog: str, schema: str, name: str, fmt: str | None,
                   table_type: str | None = None) -> int | None:
    """Return sizeInBytes via DESCRIBE DETAIL, or None.

    Treats Delta, null/empty fmt, and MATERIALIZED_VIEW / STREAMING_TABLE as
    Delta-capable. UC's information_schema.tables.data_source_format is NULL
    for managed tables and "UNKNOWN_DATA_SOURCE_FORMAT" for MVs/STs, so we
    must look at table_type as a secondary signal. MVs and STs are
    Delta-backed under the hood — DESCRIBE DETAIL works on them and returns
    a valid sizeInBytes. Genuinely non-Delta tables get rejected by
    DESCRIBE DETAIL itself; we catch and log.
    """
    if not COLLECT_SIZES:
        return None
    fmt_upper = (fmt or "").upper()
    fqn_display = f"{catalog}.{schema}.{name}"
    # MVs and STs are treated as views by DESCRIBE DETAIL (it errors with
    # EXPECT_TABLE_NOT_VIEW). Their sizes are not directly queryable; the
    # backing __materialization_mat_* tables (already in inventory as MANAGED
    # Delta) carry the real bytes. Skip MV/ST cleanly with a clear reason.
    if table_type in {"MATERIALIZED_VIEW", "STREAMING_TABLE"}:
        _size_skipped.append((
            fqn_display, fmt or "(null)",
            f"{table_type}: DESCRIBE DETAIL not supported; size is in backing __materialization_* table",
        ))
        return None
    delta_capable_table_types = {"MANAGED", "EXTERNAL"}
    is_delta_compat_fmt = (not fmt_upper) or fmt_upper == "DELTA"
    is_delta_compat_type = table_type in delta_capable_table_types
    # Skip only when both fmt and table_type say this is definitely not Delta-capable.
    if not is_delta_compat_fmt and not is_delta_compat_type:
        _size_skipped.append((fqn_display, fmt or "(null)", f"non-Delta format: {fmt_upper}"))
        return None
    try:
        row = spark.sql(f"DESCRIBE DETAIL {quote_fqn(catalog, schema, name)}").first()
        if row is None:
            _size_skipped.append((fqn_display, fmt or "(null)", "DESCRIBE DETAIL returned no row"))
            return None
        d = row.asDict()
        if "sizeInBytes" not in d or d["sizeInBytes"] is None:
            _size_skipped.append((fqn_display, fmt or "(null)", "sizeInBytes column absent or null"))
            return None
        return int(d["sizeInBytes"])
    except Exception as e:
        _size_skipped.append((fqn_display, fmt or "(null)", f"{type(e).__name__}: {str(e)[:120]}"))
        return None


records: list[tuple[ObjectRecord, str]] = []
_fallback_count = 0

for _, row in tables_df.iterrows():
    cat, sch, nm = row["table_catalog"], row["table_schema"], row["table_name"]
    fmt = row.get("data_source_format")

    raw_path = row.get("storage_path")
    storage_path = resolve_storage_path(
        spark=spark, catalog=cat, schema=sch, name=nm,
        info_schema_path=raw_path, object_type="TABLE",
    )
    if storage_path != raw_path and storage_path is not None:
        _fallback_count += 1

    rec = ObjectRecord(
        catalog=cat, schema=sch, name=nm,
        object_type="TABLE",
        table_type=row["table_type"],
        data_source_format=fmt,
        storage_path=storage_path,
        parent_managed_location=parent_managed_location(cat, sch),
        owner=row.get("owner"),
        created_at=row.get("created"),
        last_altered=row.get("last_altered"),
        requires_pipeline_handling=_requires_pipeline_handling(row["table_type"]),
        size_bytes=_collect_size(cat, sch, nm, fmt, table_type=row["table_type"]),
        tag_count=tag_counts.get((cat, sch, nm)),
        grant_count=grant_counts.get((cat, sch, nm)),
        has_row_filter=None,    # captured in Plan 2's metadata snapshot
        has_column_mask=None,
    )
    cls = classify_object(rec, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT)
    records.append((rec, cls))

for _, row in volumes_df.iterrows():
    cat, sch, nm = row["table_catalog"], row["table_schema"], row["table_name"]
    raw_path = row.get("storage_path")
    storage_path = resolve_storage_path(
        spark=spark, catalog=cat, schema=sch, name=nm,
        info_schema_path=raw_path, object_type="VOLUME",
    )
    if storage_path != raw_path and storage_path is not None:
        _fallback_count += 1

    vol_fqn = f"{cat}.{sch}.{nm}"
    rec = ObjectRecord(
        catalog=cat, schema=sch, name=nm,
        object_type="VOLUME",
        table_type=row["table_type"],
        data_source_format=None,
        storage_path=storage_path,
        parent_managed_location=parent_managed_location(cat, sch),
        owner=row.get("owner"),
        created_at=row.get("created"),
        last_altered=row.get("last_altered"),
        requires_pipeline_handling=False,
        size_bytes=_collect_volume_size(cat, sch, nm, vol_fqn),
        tag_count=None,
        grant_count=None,
        has_row_filter=None,
        has_column_mask=None,
    )
    cls = classify_object(rec, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT)
    records.append((rec, cls))

# Registered models — classify_object routes them to requires_external_handling.
for _, row in models_df.iterrows():
    cat, sch, nm = row["table_catalog"], row["table_schema"], row["table_name"]
    rec = ObjectRecord(
        catalog=cat, schema=sch, name=nm,
        object_type="REGISTERED_MODEL",
        table_type=None, data_source_format=None,
        storage_path=None, parent_managed_location=parent_managed_location(cat, sch),
        owner=row.get("owner"),
        created_at=row.get("created"), last_altered=row.get("last_altered"),
        requires_pipeline_handling=False,
        size_bytes=None, tag_count=None, grant_count=None,
        has_row_filter=None, has_column_mask=None,
    )
    records.append((rec, classify_object(rec, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT)))

# Functions / UDFs — same.
for _, row in functions_df.iterrows():
    cat, sch, nm = row["table_catalog"], row["table_schema"], row["table_name"]
    rec = ObjectRecord(
        catalog=cat, schema=sch, name=nm,
        object_type="FUNCTION",
        table_type=None, data_source_format=None,
        storage_path=None, parent_managed_location=parent_managed_location(cat, sch),
        owner=row.get("owner"),
        created_at=row.get("created"), last_altered=row.get("last_altered"),
        requires_pipeline_handling=False,
        size_bytes=None, tag_count=None, grant_count=None,
        has_row_filter=None, has_column_mask=None,
    )
    records.append((rec, classify_object(rec, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT)))

print(f"Classified {len(records)} objects")
print(f"DESCRIBE EXTENDED fallback resolved {_fallback_count} objects that had null info_schema.storage_path")

if COLLECT_SIZES:
    sized = sum(1 for r, _ in records if r.size_bytes is not None)
    print(f"Sizes collected for {sized} / {len(records)} objects (skipped: {len(_size_skipped)})")
    if _size_skipped:
        # Group skip reasons for a compact summary
        from collections import Counter
        reasons = Counter(s[2].split(":")[0] for s in _size_skipped)
        print("  skip reasons (top):")
        for reason, n in reasons.most_common(5):
            print(f"    {reason}: {n}")
        print("  first 5 skipped objects:")
        for fqn, fmt, reason in _size_skipped[:5]:
            print(f"    {fqn} (fmt={fmt}): {reason}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — Write inventory Delta table

# COMMAND ----------
writer = InventoryWriter(spark=spark)
inv_df = writer.records_to_dataframe(records)
writer.overwrite_delta(inv_df, table_name=f"{OPS_SCHEMA}.inventory")
print(f"Wrote {inv_df.count()} rows to {OPS_SCHEMA}.inventory")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6 — Downstream consumers (lineage)

# COMMAND ----------
lineage_sql = build_lineage_consumers_query(
    inventory_table=f"{OPS_SCHEMA}.inventory",
    days=LINEAGE_LOOKBACK_DAYS,
)
try:
    lineage_df = spark.sql(lineage_sql)
    lineage_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{OPS_SCHEMA}.lineage_consumers")
    print(f"Wrote {lineage_df.count()} lineage edges")
except Exception as e:
    print(f"Lineage query failed (system.access may not be enabled): {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 7 — Summary

# COMMAND ----------
# Sum bytes on new-storage for the cost signal in the recommendation
bytes_on_new = sum(
    r.size_bytes or 0
    for r, c in records
    if c == "consistent_new"
)

rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=bytes_on_new)
md = render_summary_markdown(records=records, recommendation=rec)
displayHTML(f"<pre>{md}</pre>")  # noqa: F821 (Databricks builtin)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 8 — Permission audit
# MAGIC
# MAGIC Checks whether the current user has the privileges required to run
# MAGIC the remaining notebooks (`00_repoint_schemas`, `03b_forward_migrate`,
# MAGIC `03a_rollback`, `04_validation`, `05_cleanup`). Reports deficits by
# MAGIC object — does NOT raise. Discovery itself succeeded, so anything
# MAGIC reported here is about the *destructive* notebooks.
# MAGIC
# MAGIC The audit checks:
# MAGIC - Object ownership (owners have full control)
# MAGIC - Direct + group grants via `SHOW GRANTS ON <securable>`
# MAGIC - Catalogs need: `USE CATALOG`, `CREATE SCHEMA` (for ops schema)
# MAGIC - Schemas need: `USE SCHEMA`, `CREATE TABLE`, plus `MANAGE` for native-UC repoint
# MAGIC - Tables need: OWNER (for `RENAME`/`DROP` in `03b`/`03a`/`05`)
# MAGIC - External locations matching NEW: `CREATE EXTERNAL TABLE`, `READ FILES`, `WRITE FILES`
# MAGIC
# MAGIC Caveats: `SHOW GRANTS` shows what's bound to specific principals; if
# MAGIC a user has access via a group not surfaced by `current_user.me()`,
# MAGIC the audit may flag a false-positive deficit. Treat the output as
# MAGIC "things to verify", not absolute truth.

# COMMAND ----------
print("=" * 70)
print("Permission audit")
print("=" * 70)

me_info = w.current_user.me()
my_user = me_info.user_name
my_groups = {g.display for g in (me_info.groups or [])} if getattr(me_info, "groups", None) else set()
my_principals = {my_user} | my_groups
print(f"Identity: {my_user}")
print(f"Groups visible via SCIM: {sorted(my_groups) if my_groups else '(none — direct grants only will be matched)'}")
print()

deficits: list[str] = []


def _my_privs(securable_kind: str, securable_name: str):
    """Return set of privileges the current user has via any of their
    surfaced principals. Returns None if SHOW GRANTS itself was denied
    (treated as 'cannot verify')."""
    try:
        rows = spark.sql(f"SHOW GRANTS ON {securable_kind} {securable_name}").collect()
    except Exception:
        return None
    privs = set()
    for r in rows:
        d = {k.lower(): v for k, v in r.asDict().items()}
        principal = d.get("principal") or ""
        priv = d.get("actiontype") or d.get("privilege")
        if principal in my_principals and priv:
            privs.add(priv)
    return privs


def _check(actual: "set | None", required: set, label: str):
    """Return (ok, message)."""
    if actual is None:
        return False, f"could not enumerate grants for {label} (SHOW GRANTS denied)"
    if "ALL PRIVILEGES" in actual:
        return True, ""
    missing = required - actual
    if missing:
        return False, f"{label}: missing {sorted(missing)}"
    return True, ""


# ---- Catalog audit ----
print("--- Catalogs ---")
for c in catalogs:
    if c.catalog_type in {"FOREIGN_CATALOG", "DELTASHARING_CATALOG", "SYSTEM_CATALOG"}:
        continue
    if c.owner == my_user:
        print(f"  {c.name}: OWNER ✓")
        continue
    privs = _my_privs("CATALOG", f"`{c.name}`")
    ok, msg = _check(privs, {"USE CATALOG", "CREATE SCHEMA"}, f"CATALOG {c.name}")
    if ok:
        print(f"  {c.name}: USE+CREATE SCHEMA ✓ (owner={c.owner})")
    else:
        print(f"  {c.name}: ✗ {msg}")
        deficits.append(msg)

# ---- Schema audit ----
print("\n--- Schemas (user-owned only; system/tool schemas skipped) ---")
schema_required = {"USE SCHEMA", "CREATE TABLE"}
schema_required_native = schema_required | {"MANAGE"}  # MANAGE needed for REST PATCH storage_root
for cat_name, schemas in schemas_by_catalog.items():
    cat_type = next((c.catalog_type for c in catalogs if c.name == cat_name), None)
    is_native_uc = cat_type == "MANAGED_CATALOG"
    required = schema_required_native if is_native_uc else schema_required
    for s in schemas:
        if s.name.startswith("_") or s.name in ("information_schema", "default"):
            continue
        full = f"{cat_name}.{s.name}"
        if s.owner == my_user:
            print(f"  {full}: OWNER ✓")
            continue
        privs = _my_privs("SCHEMA", f"`{cat_name}`.`{s.name}`")
        ok, msg = _check(privs, required, f"SCHEMA {full}")
        if ok:
            print(f"  {full}: ✓ (owner={s.owner})")
        else:
            print(f"  {full}: ✗ {msg}")
            deficits.append(msg)

# ---- Table ownership audit ----
# Tables need OWNER for RENAME (03b) and DROP (03a/05). Group by owner so we
# don't spam thousands of lines for non-owners.
print("\n--- Table ownership ---")
in_scope_tables = tables_df  # already filtered by catalog allowlist
non_owned = in_scope_tables[in_scope_tables["owner"] != my_user]
if len(non_owned) == 0:
    print(f"  All {len(in_scope_tables)} in-scope tables owned by {my_user} ✓")
else:
    by_owner = non_owned.groupby("owner").size().to_dict()
    print(f"  {len(non_owned)} of {len(in_scope_tables)} table(s) not owned by you:")
    for owner, n in sorted(by_owner.items(), key=lambda x: -x[1]):
        print(f"    owner={owner}: {n} table(s)")
    deficits.append(
        f"{len(non_owned)} table(s) lack OWNER privilege. "
        f"Owners: {dict(sorted(by_owner.items(), key=lambda x: -x[1]))}. "
        "03b/03a/05 require OWNER (for RENAME/DROP). Either transfer ownership, "
        "join the owning group, or run those notebooks as the table owner."
    )

# ---- External location audit (NEW) ----
print("\n--- External locations matching NEW storage ---")
new_token = NEW_STORAGE_ACCOUNT.split("/")[0]  # bucket or account portion, in case of prefix mode
ext_locs_new = [el for el in ext_locs if new_token in (el.url or "")]
if not ext_locs_new:
    msg = (f"No external location URL contains '{new_token}'. "
           "03b managed Delta DEEP CLONE writes through an external location bound "
           "to the schema's storage_root; create one before running 03b.")
    print(f"  ✗ {msg}")
    deficits.append(msg)
else:
    ext_required = {"CREATE EXTERNAL TABLE", "READ FILES", "WRITE FILES"}
    for el in ext_locs_new:
        if el.owner == my_user:
            print(f"  {el.name} ({el.url}): OWNER ✓")
            continue
        privs = _my_privs("EXTERNAL LOCATION", f"`{el.name}`")
        ok, msg = _check(privs, ext_required, f"EXTERNAL LOCATION {el.name}")
        if ok:
            print(f"  {el.name}: ✓ (owner={el.owner})")
        else:
            print(f"  {el.name}: ✗ {msg}")
            deficits.append(msg)

# ---- Summary ----
print("\n" + "=" * 70)
if not deficits:
    print("Permission audit: PASS — no deficits detected. ✓")
else:
    print(f"Permission audit: {len(deficits)} potential deficit(s) — review before running 03b/03a/05.")
    print()
    for i, d in enumerate(deficits, 1):
        print(f"  {i}. {d}")
    print()
    print("Note: SHOW GRANTS surfaces direct + group grants for principals returned by")
    print("current_user.me(). If you have access via an unsurfaced group, deficits above")
    print("may be false positives — try a dry run of the relevant notebook to confirm.")
print("=" * 70)

# ---- Remediation: ready-to-run GRANT statements for missing privileges ----
# Turns the free-text privilege deficits above into copy-paste SQL. Run these
# AS A PRINCIPAL THAT CAN GRANT (securable owner / metastore admin). Table
# OWNER deficits aren't covered here — those need ALTER ... OWNER TO or group
# membership, not a GRANT (see the table-ownership deficit text above).
import re as _re

_KIND_GRANTABLE = {"CATALOG": "CATALOG", "SCHEMA": "SCHEMA", "EXTERNAL LOCATION": "EXTERNAL LOCATION"}
_grant_sql: list[str] = []
for d in deficits:
    m = _re.match(r"^(CATALOG|SCHEMA|EXTERNAL LOCATION) (.+?): missing (\[.*\])$", d)
    if not m:
        continue
    kind, name, privs_repr = m.group(1), m.group(2), m.group(3)
    try:
        privs = [p for p in __import__("ast").literal_eval(privs_repr)]
    except Exception:
        continue
    # Quote each dotted identifier component: cat.schema -> `cat`.`schema`
    qname = ".".join(f"`{part}`" for part in name.split("."))
    _grant_sql.append(f"GRANT {', '.join(privs)} ON {_KIND_GRANTABLE[kind]} {qname} TO `{my_user}`;")

if _grant_sql:
    print("\nSuggested GRANTs to clear the privilege deficits above")
    print("(run as a securable owner or metastore admin; review before executing):")
    print("-" * 70)
    for g in _grant_sql:
        print(g)
    print("-" * 70)

# Heads-up for managed-volume migration (03b Step 6.5): in addition to the
# table privileges audited above, the runner needs `CREATE VOLUME` on each
# target schema and OWNER on the managed volumes (for DROP + RENAME during the
# staging swap). Volumes are not individually audited here.
if any(getattr(r, "object_type", None) == "VOLUME" and getattr(r, "table_type", None) == "MANAGED"
       for r in records):
    print("\nManaged volumes are in scope: ensure you also hold CREATE VOLUME on the "
          "target schema(s) and OWNER on those volumes before running 03b.")
