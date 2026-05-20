"""Single source of truth for migration configuration.

Edit the values in this file ONCE before running any notebook. Each
notebook imports the values it needs — no per-notebook config cells to
keep in sync.

What lives here: identity values (account names, ops_schema), scope
(catalog/schema lists), and tunables (thresholds, cost coefficients,
sample limits).

What does NOT live here: per-run operational gates. Each mutating
notebook keeps its own `CONFIRMED`, `DRY_RUN`, `ACTOR`, and
`POST_VALIDATION_CLEANUP_OK` flags so a single edit to this file can't
accidentally arm destructive operations across the whole pipeline.
"""
from __future__ import annotations


# =============================================================================
# Storage account / prefix identifiers
# =============================================================================

# Azure: the bare storage-account name embedded in the abfss URL (e.g.,
# "oldacct" for abfss://container@oldacct.dfs.core.windows.net).
#
# AWS S3 (bucket mode): the bare S3 bucket name (e.g., "my-bucket").
#
# AWS S3 (single-bucket prefix mode, for AWS testing only): the form
# "bucket/prefix" — e.g., "my-bucket/__unitystorage". This is the
# AWS-port escape hatch when both OLD and NEW need to live in the same
# bucket under different prefixes. See utils/paths.py:classify_url for
# semantics.
OLD_STORAGE_ACCOUNT: str = "oldacct"
NEW_STORAGE_ACCOUNT: str = "newacct"


# =============================================================================
# UC objects in scope
# =============================================================================

# Schema (catalog.schema_name) where the migration's audit + state
# tables (inventory, external_locations, lineage_consumers,
# migration_log, validation_results, object_metadata_snapshot) live.
# Pick a catalog the user owns / has CREATE SCHEMA on.
OPS_SCHEMA: str = "main._migration_ops"

# Catalogs in scope for discovery + migration. Empty list = all
# catalogs in the metastore (typically not what you want for a
# production migration — narrow this down).
CATALOG_ALLOWLIST: list[str] = []


# =============================================================================
# Native UC repoint setup — used by 00_repoint_schemas only
# =============================================================================

# Catalog whose schemas to repoint before running 01_discovery. Native
# UC catalogs block SQL `ALTER SCHEMA SET MANAGED LOCATION`, so we use
# the UC REST PATCH endpoint to set storage_root. See utils/uc_admin.py.
# HMS-federated catalogs don't need 00_repoint_schemas — they can use
# the SQL form directly.
REPOINT_CATALOG: str = ""

# Schemas under REPOINT_CATALOG that should have their storage_root
# repointed to NEW_STORAGE_PREFIX. Typically the user-owned schemas;
# omit `information_schema`, the OPS_SCHEMA's containing schema if any,
# and any system schemas.
SCHEMAS_TO_REPOINT: list[str] = []

# Full URL prefix where each repointed schema's storage_root will be
# set. The notebook appends `/<schema>` per schema, so set this to the
# common parent. Example:
#   "abfss://container@newacct.dfs.core.windows.net/path/<catalog>"
#   "s3://new-bucket/path/<catalog>"
NEW_STORAGE_PREFIX: str = ""


# =============================================================================
# Discovery tunables (01_discovery)
# =============================================================================

# Populate `size_bytes` via DESCRIBE DETAIL for managed Delta tables and
# via dbutils.fs.ls walks for volumes. Set False to skip sizing entirely
# (faster discovery; cost-estimate becomes useless).
COLLECT_SIZES: bool = True

# How far back to look for downstream-consumer lineage edges.
LINEAGE_LOOKBACK_DAYS: int = 30


# =============================================================================
# Decision-report thresholds + cost-estimate constants (02_decision_report)
# =============================================================================

# Above any of these and the recommendation flips to
# FORWARD_MIGRATE_REQUIRED instead of ROLLBACK_FEASIBLE.
THRESHOLDS: dict = {
    "max_consistent_new_objects": 25,
    "max_bytes_on_new_gb": 10.0,
    "max_distinct_owners_on_new": 3,
    "max_age_days_on_new": 30,
}

# Rule-of-thumb GB/sec for the duration estimate. ADLS-to-ADLS in the
# same region is the optimistic case; cross-region is the conservative
# case. Adjust if you have empirical numbers from a Mosaic AI POC.
ADLS_CLONE_GBPS_SAME_REGION: float = 0.5
ADLS_CLONE_GBPS_CROSS_REGION: float = 0.15

# DBU rate for the cluster running the migration. Used only for the
# rough cost estimate printed by 02_decision_report.
DBU_PER_HOUR: float = 1.5


# =============================================================================
# Validation tunables (04_validation)
# =============================================================================

# Number of rows to sample per table for the input-file-path evidence
# layer (Layer 3 in validate_object_on_new). Higher = more confidence;
# lower = faster validation across thousands of tables.
SAMPLE_LIMIT: int = 10000
