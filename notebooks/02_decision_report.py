# Databricks notebook source
# MAGIC %md
# MAGIC # 02_decision_report — Rollback vs forward-migrate recommendation
# MAGIC
# MAGIC **Purpose:** Read `<OPS_SCHEMA>.inventory` (produced by `01_discovery`) and
# MAGIC produce an opinionated recommendation: ROLLBACK_FEASIBLE,
# MAGIC ROLLBACK_REQUIRES_SIGNOFF, or FORWARD_MIGRATE_REQUIRED.
# MAGIC
# MAGIC **Inputs:** `<OPS_SCHEMA>.inventory`, `<OPS_SCHEMA>.external_locations`.
# MAGIC
# MAGIC **Outputs:** Markdown summary, rollback-cost ledger, cost/time estimate.
# MAGIC No tables written.
# MAGIC
# MAGIC **Side effects:** None. Read-only.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Path setup — make `utils/` importable

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
            print(f"Added {parent} to sys.path for utils/ imports")
            return
        candidate = parent
    print("WARNING: could not auto-locate utils/; set _REPO_ROOT_HINT below.")


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
from utils.config import (
    OPS_SCHEMA,
    THRESHOLDS,
    ADLS_CLONE_GBPS_SAME_REGION,
    ADLS_CLONE_GBPS_CROSS_REGION,
    DBU_PER_HOUR,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load inventory

# COMMAND ----------
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
        requires_pipeline_handling=r["requires_pipeline_handling"],
        size_bytes=r["size_bytes"],
        tag_count=r["tag_count"],
        grant_count=r["grant_count"],
        has_row_filter=r["has_row_filter"],
        has_column_mask=r["has_column_mask"],
    )
    records.append((rec, r["classification"]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Compute recommendation

# COMMAND ----------
bytes_on_new = sum(r.size_bytes or 0 for r, c in records if c == "consistent_new")
thresholds = DecisionThresholds(**THRESHOLDS)
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
        "size_bytes": r.size_bytes,
    } for r in new_objects])
    display(df)  # noqa: F821

# COMMAND ----------
# MAGIC %md
# MAGIC ## Forward-migrate cost/time estimate

# COMMAND ----------
drift = [r for r, c in records if c == "drift_managed_on_old"]
external_old = [r for r, c in records if c == "external_on_old"]

drift_bytes = sum(r.size_bytes or 0 for r in drift)
drift_gb = drift_bytes / (1024 ** 3)

# Cross-region detection: compare regions across external_locations table
ext_loc_df = spark.table(f"{OPS_SCHEMA}.external_locations")
regions = {r["region"] for r in ext_loc_df.collect() if r["region"]}
cross_region = len(regions) > 1
gbps = ADLS_CLONE_GBPS_CROSS_REGION if cross_region else ADLS_CLONE_GBPS_SAME_REGION
duration_hours = (drift_gb / gbps / 3600) if drift_gb > 0 else 0
estimated_dbu = duration_hours * DBU_PER_HOUR

print(f"Managed objects to clone (drift): {len(drift)}")
print(f"External objects to re-point:     {len(external_old)}")
print(f"Bytes to clone:                   {drift_bytes:,} bytes ({drift_gb:.2f} GB)")
print(f"Regions seen in external_locations: {sorted(regions) or '[none]'}")
print(f"Cross-region migration:           {cross_region}")
print(f"Estimated clone duration:         {duration_hours:.2f} hours "
      f"(@ {gbps} GB/sec)")
print(f"Estimated DBU cost:               {estimated_dbu:.2f} DBU "
      f"(@ {DBU_PER_HOUR} DBU/hour for a single all-purpose cluster)")
print()
print("Note: size_bytes is populated only for Delta tables when COLLECT_SIZES=True "
      "during discovery. Non-Delta tables and external locations are excluded "
      "from the byte total; the duration estimate is therefore a lower bound.")
