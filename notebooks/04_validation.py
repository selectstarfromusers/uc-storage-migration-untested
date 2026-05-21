# Databricks notebook source
# MAGIC %md
# MAGIC # 04_validation — Four-layer evidence for every migrated object
# MAGIC
# MAGIC **Purpose:** For every object that migrated, prove via four independent
# MAGIC evidence layers that queries genuinely read from new storage.
# MAGIC
# MAGIC **Inputs:** `<OPS_SCHEMA>.migration_log`, `<OPS_SCHEMA>.inventory`.
# MAGIC
# MAGIC **Outputs:** `<OPS_SCHEMA>.validation_results` — one row per object with
# MAGIC all evidence flags and raw evidence JSON.
# MAGIC
# MAGIC **Side effects:** Read-only against UC objects; writes only to
# MAGIC `<OPS_SCHEMA>.validation_results`.

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
# All values come from utils/config.py — edit there, not here.
from utils import config as _cfg
_cfg.resolve_config(spark=spark)
NEW_STORAGE_ACCOUNT = _cfg.NEW_STORAGE_ACCOUNT
OPS_SCHEMA = _cfg.OPS_SCHEMA
SAMPLE_LIMIT = _cfg.SAMPLE_LIMIT

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run validation

# COMMAND ----------
from datetime import datetime, timezone

from utils.validation import validate_object_on_new, evidence_to_json
from utils.state import VALIDATION_RESULTS_SCHEMA, ValidationResultsWriter


validated_rows = spark.sql(
    f"SELECT m.catalog, m.schema, m.name, m.object_type, i.data_source_format, "
    f"       i.parent_managed_location, i.table_type "
    f"FROM {OPS_SCHEMA}.migration_log m "
    f"JOIN {OPS_SCHEMA}.inventory i "
    f"  ON m.catalog = i.catalog AND m.schema = i.schema AND m.name = i.name "
    f"WHERE m.status = 'validated'"
).collect()

print(f"Validating {len(validated_rows)} migrated objects...")

writer = ValidationResultsWriter(spark=spark, table_name=f"{OPS_SCHEMA}.validation_results")
writer.ensure_exists()

fs = dbutils.fs  # noqa: F821
results_rows = []
for r in validated_rows:
    result = validate_object_on_new(
        spark=spark, fs=fs,
        catalog=r["catalog"], schema=r["schema"], name=r["name"],
        expected_new_account=NEW_STORAGE_ACCOUNT,
        parent_managed_location=r["parent_managed_location"],
        is_delta=(r["data_source_format"] == "DELTA"),
        sample_limit=SAMPLE_LIMIT,
        is_external=(r["table_type"] == "EXTERNAL"),
    )
    results_rows.append((
        result.catalog, result.schema, result.name,
        result.metadata_location_ok, result.delta_log_at_new_ok,
        result.input_file_name_ok, result.parent_managed_location_match,
        None, None, None, None, None, None,   # governance flags — Plan 2.1 expansion
        result.overall_pass,
        evidence_to_json(result),
        result.validated_at,
    ))
    print(f"  {result.catalog}.{result.schema}.{result.name}: "
          f"overall_pass={result.overall_pass} "
          f"(meta={result.metadata_location_ok} "
          f"delta_log={result.delta_log_at_new_ok} "
          f"input_file={result.input_file_name_ok} "
          f"parent={result.parent_managed_location_match})")

if results_rows:
    writer.overwrite(results_rows)
    _OVERALL_PASS_IDX = VALIDATION_RESULTS_SCHEMA.fieldNames().index("overall_pass")
    pass_count = sum(1 for r in results_rows if r[_OVERALL_PASS_IDX])
    print(f"\n{pass_count} / {len(results_rows)} passed all four evidence layers.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Failure detail

# COMMAND ----------
spark.sql(
    f"SELECT catalog, schema, name, metadata_location_ok, delta_log_at_new_ok, "
    f"       input_file_name_ok, parent_managed_location_match, evidence_json "
    f"FROM {OPS_SCHEMA}.validation_results "
    f"WHERE NOT overall_pass"
).show(truncate=False)
