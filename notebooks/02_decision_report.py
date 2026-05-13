# Databricks notebook source
# MAGIC %md
# MAGIC # 02_decision_report — Rollback vs forward-migrate recommendation
# MAGIC
# MAGIC **Purpose:** Read `<OPS_SCHEMA>.inventory` (produced by `01_discovery`) and
# MAGIC produce an opinionated recommendation: ROLLBACK_FEASIBLE,
# MAGIC ROLLBACK_REQUIRES_SIGNOFF, or FORWARD_MIGRATE_REQUIRED.
# MAGIC
# MAGIC **Inputs:** `<OPS_SCHEMA>.inventory`.
# MAGIC
# MAGIC **Outputs:** Markdown summary, rollback-cost ledger, cost/time estimate.
# MAGIC No tables written.
# MAGIC
# MAGIC **Side effects:** None. Read-only.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
OPS_SCHEMA = "main._migration_ops"
# Thresholds tunable here without re-running discovery
THRESHOLDS = {
    "max_consistent_new_objects": 25,
    "max_bytes_on_new_gb": 10.0,
    "max_distinct_owners_on_new": 3,
    "max_age_days_on_new": 30,
}
# Rule-of-thumb for cost/time estimate
ADLS_CLONE_GBPS = 0.5   # GB/sec, conservative same-region estimate
DBU_PER_HOUR = 1.5      # cluster DBU rate

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load inventory

# COMMAND ----------
from datetime import datetime

from utils.discovery import ObjectRecord
from utils.reporting import (
    DecisionThresholds, compute_recommendation, render_summary_markdown,
)

inv_df = spark.table(f"{OPS_SCHEMA}.inventory")
print(f"Inventory rows: {inv_df.count()}")

rows = inv_df.collect()
records = []
for r in rows:
    rec = ObjectRecord(
        catalog=r["catalog"], schema=r["schema"], name=r["name"],
        object_type=r["object_type"], table_type=r["table_type"],
        data_source_format=r["data_source_format"],
        storage_path=r["storage_path"],
        parent_managed_location=r["parent_managed_location"],
        owner=r["owner"],
        created_at=r["created_at"], last_altered=r["last_altered"],
    )
    records.append((rec, r["classification"]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Compute recommendation

# COMMAND ----------
thresholds = DecisionThresholds(**THRESHOLDS)
bytes_on_new = 0  # Wire to actual size collection in a later iteration; placeholder for now.
rec = compute_recommendation(records, thresholds=thresholds, bytes_on_new=bytes_on_new)

md = render_summary_markdown(records=records, recommendation=rec)
displayHTML(f"<pre>{md}</pre>")  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ## Rollback-cost ledger
# MAGIC
# MAGIC If rollback is chosen, the following objects will be dropped:

# COMMAND ----------
new_objects = [r for r, c in records if c == "consistent_new"]
if not new_objects:
    print("No consistent_new objects. Rollback drops nothing.")
else:
    import pandas as pd
    df = pd.DataFrame([{
        "fqn": f"{r.catalog}.{r.schema}.{r.name}",
        "object_type": r.object_type,
        "owner": r.owner,
        "created_at": r.created_at,
    } for r in new_objects])
    display(df)  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ## Forward-migrate cost/time estimate

# COMMAND ----------
drift = [r for r, c in records if c == "drift_managed_on_old"]
external_old = [r for r, c in records if c == "external_on_old"]
print(f"Managed objects to clone: {len(drift)}")
print(f"External objects to re-point: {len(external_old)}")
print()
print(f"Bytes to clone: TODO (collect via DESCRIBE DETAIL when COLLECT_SIZES=True in discovery)")
print(f"Estimated clone duration (rule of thumb): TODO")
print(f"Estimated DBU cost: TODO")
