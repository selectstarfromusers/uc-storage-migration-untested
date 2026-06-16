"""Single source of truth for migration configuration.

Edit the values in this file ONCE before running any notebook. Each
notebook imports the values it needs — no per-notebook config cells to
keep in sync.

Smart defaults: some values (OPS_SCHEMA, REPOINT_CATALOG,
SCHEMAS_TO_REPOINT) can be auto-derived from CATALOG_ALLOWLIST at
notebook startup. Leave them at their `None` sentinel to opt in. Set
them explicitly to override.

Per-run operational gates (`CONFIRMED`, `DRY_RUN`, `ACTOR`) deliberately
stay in each mutating notebook so a single edit to this file can't arm
destructive ops across the whole pipeline.

Cleanup is gated by `POST_VALIDATION_CLEANUP_OK` here AND
`DRY_RUN=False` in the cleanup notebook (`05_cleanup`). Two gates —
config + notebook — because cleanup is irreversible.
"""
from __future__ import annotations

from typing import Optional


# =============================================================================
# Repo root hint (used by notebooks' path-setup cells)
# =============================================================================
#
# Chicken-and-egg note: each notebook needs to add the repo root to sys.path
# BEFORE it can `import utils.config`. So the notebook's path-setup cell
# auto-discovers utils/ by walking up the filesystem first (using the
# notebook's own workspace path via dbutils.notebook.entry_point). Only
# AFTER that auto-discovery succeeds do the notebooks consult
# REPO_ROOT_HINT here as a customer-set override.
#
# Set this only when auto-discovery doesn't work for your workspace layout.
# Leave at None in the typical case where notebooks/ and utils/ are siblings.
REPO_ROOT_HINT: Optional[str] = None


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
# migration_log, validation_results, object_metadata_snapshot,
# cleanup_log) live.
#
# AUTO-DERIVABLE: leave at None to auto-fill as
# f"{CATALOG_ALLOWLIST[0]}._migration_ops" at notebook startup. Set
# explicitly to override.
OPS_SCHEMA: Optional[str] = None

# Catalogs in scope for discovery + migration. REQUIRED — empty list
# is refused unless ALLOW_ALL_CATALOGS=True (see below).
CATALOG_ALLOWLIST: list[str] = []

# Escape hatch for "migrate every catalog in the metastore". Defaults
# False because empty-allowlist-means-all is the kind of footgun that
# starts a migration of the system catalog before anyone realizes.
ALLOW_ALL_CATALOGS: bool = False


# =============================================================================
# Native UC repoint setup — used by 00_repoint_schemas only
# =============================================================================

# Catalog whose schemas to repoint before running 01_discovery. Native
# UC catalogs block SQL `ALTER SCHEMA SET MANAGED LOCATION`, so we use
# the UC REST PATCH endpoint to set storage_root. See utils/uc_admin.py.
# HMS-federated catalogs don't need 00_repoint_schemas — they can use
# the SQL form directly.
#
# AUTO-DERIVABLE: leave at None to auto-fill from CATALOG_ALLOWLIST
# when len(CATALOG_ALLOWLIST) == 1. Refuses to auto-derive if the
# allowlist has multiple catalogs (you must pick one explicitly).
REPOINT_CATALOG: Optional[str] = None

# Schemas under REPOINT_CATALOG that should have their storage_root
# repointed to NEW_STORAGE_PREFIX. Typically the user-owned schemas.
#
# AUTO-DERIVABLE: leave at None to auto-populate with every user-owned
# schema in REPOINT_CATALOG, EXCLUDING `information_schema`, `default`,
# any schema starting with `_` (convention for tool-owned), and the
# schema portion of OPS_SCHEMA if it lives in REPOINT_CATALOG.
SCHEMAS_TO_REPOINT: Optional[list[str]] = None

# Full URL prefix where each repointed schema's storage_root will be
# set. The notebook appends `/<schema>` per schema, so set this to the
# common parent. REQUIRED. Examples:
#   "abfss://container@newacct.dfs.core.windows.net/path/<catalog>"
#   "s3://new-bucket/path/<catalog>"
#
# Not auto-derivable: knowing the container name (Azure) or full S3
# prefix path requires customer input. Notebook errors with a clear
# message if not set.
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
# Forward-migrate tunables (03b_forward_migrate)
# =============================================================================

# Behavior when discovery finds MANAGED VOLUMEs in scope. 03b now migrates
# them in Step 6.5 via a staging-swap (create new managed volume → copy files
# → verify count/size/paths → rename swap → replay grants; original kept as
# `__pre_migration`, integrity mismatch blocks that volume).
#   False (default): migrate managed volumes in Step 6.5.
#   True: skip managed volumes (table-only migration); they are listed only.
ALLOW_MANAGED_VOLUMES_SKIP: bool = False

# Managed-volume copy strategy (Step 6.5). A single sequential
# `dbutils.fs.cp(recurse=True)` is driver-bound and per-file latency-limited:
# a volume with hundreds of thousands of small files cannot finish inside a
# job timeout. At/above LARGE_VOLUME_FILE_THRESHOLD files, the copy is fanned
# out instead of run sequentially; below it, the proven simple recurse-copy is
# kept (low overhead, already battle-tested on small volumes).
LARGE_VOLUME_FILE_THRESHOLD: int = 5000

# Parallelism for the DRIVER-side threaded copy (default mechanism — uses
# dbutils.fs.cp per file across a bounded thread pool; works everywhere,
# including serverless). dbutils.fs.cp is I/O-bound, so many threads give a
# large speedup over the sequential loop without needing executors.
VOLUME_COPY_PARALLELISM: int = 64

# Opt-in: fan the copy across Spark EXECUTORS (foreachPartition + local
# `/Volumes` FUSE copy) instead of driver threads. Faster on large
# all-purpose/dedicated clusters, but executor FUSE *writes* to UC Volumes are
# not guaranteed on every compute type (notably serverless) — validate in the
# target workspace before enabling. Default False = driver-threaded copy.
VOLUME_DISTRIBUTED_COPY: bool = False
VOLUME_DISTRIBUTED_COPY_PARTITIONS: int = 256


# =============================================================================
# Validation tunables (04_validation)
# =============================================================================

# Number of rows to sample per table for the input-file-path evidence
# layer (Layer 3 in validate_object_on_new). Higher = more confidence;
# lower = faster validation across thousands of tables.
SAMPLE_LIMIT: int = 10000

# Run a full-table content checksum during validation, comparing each migrated
# managed table against its retained `__pre_migration` shadow (order-independent
# xxhash64 fingerprint: count + bit_xor + sum). A mismatch FAILS that object's
# overall_pass (blocks). This is the strongest "data copied uncorrupted" proof,
# but it is a full scan of both copies — set False to skip for very large
# estates or time-boxed runs.
#   True (default): checksum every migrated managed table.
#   False: skip the checksum layer (marked N/A).
# N/A for external tables (no retained source to diff).
VALIDATE_CONTENT_CHECKSUM: bool = True

# Run a full-file content checksum for managed VOLUMES during validation —
# the volume analogue of VALIDATE_CONTENT_CHECKSUM above. Compares each
# migrated volume to its retained `__pre_migration` shadow file-by-file
# (md5 over bytes, matched by relpath). A mismatch FAILS that volume's
# overall_pass. This reads 100% of the bytes of BOTH copies (~2x the volume
# size), so it is OFF by default — count+size+path (compare_volume_listings,
# enforced at migration time in 03b Step 6.5) remains the always-on gate.
#   False (default): skip volume content hashing (count+size+path only).
#   True: hash in-scope files on both copies and compare.
VALIDATE_VOLUME_CONTENT_CHECKSUM: bool = False

# Sampling lever for volumes too large to fully hash. Hash only this fraction
# of files, selected DETERMINISTICALLY by a stable hash of the relpath so the
# source and target pick the SAME files; the remainder still get
# count+size+path. 1.0 = hash everything. Ignored when the flag above is False.
VOLUME_CHECKSUM_SAMPLE_FRACTION: float = 1.0

# Files larger than this are hashed in streamed chunks on the driver instead of
# loaded whole via Spark binaryFile (whose content column is capped at ~2 GB);
# avoids a MAX_LENGTH error mid-validation on big files. Bytes.
VOLUME_CHECKSUM_MAX_INMEMORY_BYTES: int = 2 * 1024 * 1024 * 1024


# =============================================================================
# Cleanup gate (05_cleanup)
# =============================================================================

# Whether `05_cleanup` is permitted to drop `__pre_migration` shadow
# tables. Required True here AND `DRY_RUN=False` in the cleanup notebook
# itself — two gates because cleanup is irreversible. Until this flips
# True, the cleanup notebook only previews.
POST_VALIDATION_CLEANUP_OK: bool = False


# =============================================================================
# Helpers — resolve auto-derivable values + validate per-notebook needs
# =============================================================================

def resolve_config(spark=None) -> None:
    """Resolve auto-derivable config values in-place.

    Call once at notebook startup, before reading config values. Mutates
    the module-level constants. Idempotent — calling repeatedly is safe.

    `spark` is required for SCHEMAS_TO_REPOINT auto-derivation (queries
    `system.information_schema.schemata`). Pass `spark` from your
    notebook context. If `spark` is None and SCHEMAS_TO_REPOINT needs
    deriving, that field stays None — callers should validate before
    using.
    """
    import sys
    cfg = sys.modules[__name__]

    # OPS_SCHEMA — auto-derive from CATALOG_ALLOWLIST[0] if not set.
    if cfg.OPS_SCHEMA is None:
        if cfg.CATALOG_ALLOWLIST:
            cfg.OPS_SCHEMA = f"{cfg.CATALOG_ALLOWLIST[0]}._migration_ops"

    # REPOINT_CATALOG — auto-derive only if exactly one catalog in scope.
    if cfg.REPOINT_CATALOG is None and len(cfg.CATALOG_ALLOWLIST) == 1:
        cfg.REPOINT_CATALOG = cfg.CATALOG_ALLOWLIST[0]

    # SCHEMAS_TO_REPOINT — auto-populate from information_schema.
    if cfg.SCHEMAS_TO_REPOINT is None and cfg.REPOINT_CATALOG and spark is not None:
        # User-owned schemas in REPOINT_CATALOG, excluding system + tool schemas.
        try:
            rows = spark.sql(
                "SELECT schema_name FROM system.information_schema.schemata "
                f"WHERE catalog_name = '{cfg.REPOINT_CATALOG}'"
            ).collect()
            ops_schema_local = cfg.OPS_SCHEMA.split(".", 1)[1] if cfg.OPS_SCHEMA and "." in cfg.OPS_SCHEMA else None
            cfg.SCHEMAS_TO_REPOINT = [
                r["schema_name"] for r in rows
                if r["schema_name"] not in ("information_schema", "default")
                and not r["schema_name"].startswith("_")
                and r["schema_name"] != ops_schema_local
            ]
        except Exception as e:
            # Best-effort; let validation fail loudly if SCHEMAS_TO_REPOINT
            # ends up empty when the customer runs 00_repoint_schemas.
            print(f"  (auto-derive of SCHEMAS_TO_REPOINT failed: {e}; set explicitly in config)")


def validate_config_for_discovery() -> None:
    """Validate config needed by 01_discovery. Raises with a clear
    message if anything is missing."""
    import sys
    cfg = sys.modules[__name__]

    if not cfg.CATALOG_ALLOWLIST and not cfg.ALLOW_ALL_CATALOGS:
        raise ValueError(
            "CATALOG_ALLOWLIST is empty. To migrate ALL catalogs in the "
            "metastore (rarely what you want), set ALLOW_ALL_CATALOGS=True "
            "in utils/config.py. Otherwise add the catalog(s) you want to "
            "migrate to CATALOG_ALLOWLIST."
        )
    if not cfg.OPS_SCHEMA:
        raise ValueError(
            "OPS_SCHEMA could not be resolved. Either set it explicitly in "
            "utils/config.py (e.g. 'your_catalog._migration_ops') or set "
            "CATALOG_ALLOWLIST so it can be auto-derived."
        )


def validate_config_for_repoint() -> None:
    """Validate config needed by 00_repoint_schemas."""
    import sys
    cfg = sys.modules[__name__]

    if not cfg.NEW_STORAGE_PREFIX:
        raise ValueError(
            "NEW_STORAGE_PREFIX is required for 00_repoint_schemas. Set "
            "it in utils/config.py to the full storage URL prefix where "
            "each schema's storage_root will be set. The notebook "
            "appends '/<schema>' per schema. Examples:\n"
            "  's3://newbucket/migration/your_catalog'\n"
            "  'abfss://container@newacct.dfs.core.windows.net/migration/your_catalog'"
        )
    if not cfg.REPOINT_CATALOG:
        raise ValueError(
            "REPOINT_CATALOG could not be resolved. Either set it "
            "explicitly in utils/config.py, or set CATALOG_ALLOWLIST to "
            "exactly one catalog so it can be auto-derived."
        )
    if not cfg.SCHEMAS_TO_REPOINT:
        raise ValueError(
            "SCHEMAS_TO_REPOINT is empty. Either set it explicitly in "
            "utils/config.py, or ensure REPOINT_CATALOG is set and "
            "resolve_config(spark=spark) ran (which auto-populates from "
            "information_schema, excluding system + tool-owned schemas)."
        )
