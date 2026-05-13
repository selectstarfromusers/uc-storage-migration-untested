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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
OLD_STORAGE_ACCOUNT = "oldacct"
NEW_STORAGE_ACCOUNT = "newacct"
CATALOG_ALLOWLIST: list[str] = []        # empty = all catalogs in metastore
OPS_SCHEMA = "main._migration_ops"
COLLECT_SIZES = True
LINEAGE_LOOKBACK_DAYS = 30

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------
from databricks.sdk import WorkspaceClient

from utils.uc_client import UcClient
from utils.discovery import ObjectRecord, classify_object
from utils.state import InventoryWriter
from utils.lineage import build_lineage_consumers_query
from utils.reporting import (
    DecisionThresholds, compute_recommendation, render_summary_markdown,
)


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
metastore = client.get_metastore()
print(f"Metastore: {metastore.name} ({metastore.metastore_id})")
print(f"  storage_root: {metastore.storage_root}")
print(f"  region: {metastore.region}")

ext_locs = client.list_external_locations()
print(f"\nExternal locations: {len(ext_locs)}")
for el in ext_locs:
    print(f"  {el.name} -> {el.url} (cred={el.credential_name}, read_only={el.read_only})")

import pandas as pd
ext_df = spark.createDataFrame(pd.DataFrame([el.__dict__ for el in ext_locs]))
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
# MAGIC ## Step 3 — Enumerate tables and volumes from information_schema

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

records: list[tuple[ObjectRecord, str]] = []

for _, row in tables_df.iterrows():
    rec = ObjectRecord(
        catalog=row["table_catalog"],
        schema=row["table_schema"],
        name=row["table_name"],
        object_type="TABLE",
        table_type=row["table_type"],
        data_source_format=row.get("data_source_format"),
        storage_path=row.get("storage_path"),
        parent_managed_location=parent_managed_location(row["table_catalog"], row["table_schema"]),
        owner=row.get("owner"),
        created_at=row.get("created"),
        last_altered=row.get("last_altered"),
    )
    cls = classify_object(rec, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT)
    records.append((rec, cls))

for _, row in volumes_df.iterrows():
    rec = ObjectRecord(
        catalog=row["table_catalog"],
        schema=row["table_schema"],
        name=row["table_name"],
        object_type="VOLUME",
        table_type=row["table_type"],
        data_source_format=None,
        storage_path=row.get("storage_path"),
        parent_managed_location=parent_managed_location(row["table_catalog"], row["table_schema"]),
        owner=row.get("owner"),
        created_at=row.get("created"),
        last_altered=row.get("last_altered"),
    )
    cls = classify_object(rec, old=OLD_STORAGE_ACCOUNT, new=NEW_STORAGE_ACCOUNT)
    records.append((rec, cls))

print(f"Classified {len(records)} objects")

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
rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=0)
md = render_summary_markdown(records=records, recommendation=rec)
displayHTML(f"<pre>{md}</pre>")  # noqa: F821 (Databricks builtin)
