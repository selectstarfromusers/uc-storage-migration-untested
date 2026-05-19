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
# MAGIC `sys.path`. If your workspace structure differs, edit `_REPO_ROOT_HINT`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Path setup — make `utils/` importable

# COMMAND ----------
import os
import sys


def _add_utils_to_path() -> None:
    """Ensure the `utils/` package is importable.

    In Databricks, a notebook's own directory is on sys.path, but its parent
    is not. This notebook lives at `<repo>/notebooks/01_discovery`; the
    `utils/` package lives at `<repo>/utils/`. We walk up from the notebook
    directory looking for a sibling `utils/` directory.
    """
    # The notebook's directory is the first sys.path entry on Databricks.
    here = sys.path[0] if sys.path else os.getcwd()
    candidate = here
    for _ in range(5):
        parent = os.path.dirname(candidate)
        if parent == candidate:
            break
        if os.path.isdir(os.path.join(parent, "utils")):
            if parent not in sys.path:
                sys.path.insert(0, parent)
            print(f"Added {parent} to sys.path for utils/ imports")
            return
        candidate = parent
    print(
        "WARNING: could not auto-locate utils/ relative to this notebook. "
        "Set _REPO_ROOT_HINT below to your repo root."
    )


_REPO_ROOT_HINT: str | None = None   # e.g., "/Workspace/Users/me@db.com/uc-storage-migration"

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
CATALOG_ALLOWLIST: list[str] = []        # empty = all catalogs in metastore
OPS_SCHEMA = "main._migration_ops"
COLLECT_SIZES = True                     # populate size_bytes via DESCRIBE DETAIL for Delta tables
LINEAGE_LOOKBACK_DAYS = 30

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
_EXT_LOC_SCHEMA = StructType([
    StructField("name", StringType(), False),
    StructField("url", StringType(), False),
    StructField("credential_name", StringType(), False),
    StructField("read_only", BooleanType(), False),
    StructField("region", StringType(), True),
])
ext_rows = [(el.name, el.url, el.credential_name, el.read_only, el.region) for el in ext_locs]
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


def _collect_size(catalog: str, schema: str, name: str, fmt: str | None) -> int | None:
    """Return sizeInBytes via DESCRIBE DETAIL, or None.

    Treats both "DELTA" and null/empty `fmt` as Delta-capable. UC's
    information_schema.tables.data_source_format is NULL for managed tables
    (the format is implicit), so a strict 'DELTA' equality check would
    silently skip every managed Delta table. DESCRIBE DETAIL itself will
    refuse if the table isn't actually Delta — we catch that exception and
    log the reason.
    """
    if not COLLECT_SIZES:
        return None
    fmt_upper = (fmt or "").upper()
    fqn_display = f"{catalog}.{schema}.{name}"
    if fmt_upper and fmt_upper != "DELTA":
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
        size_bytes=_collect_size(cat, sch, nm, fmt),
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
        size_bytes=None,
        tag_count=None,
        grant_count=None,
        has_row_filter=None,
        has_column_mask=None,
    )
    cls = classify_object(rec, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT)
    records.append((rec, cls))

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
