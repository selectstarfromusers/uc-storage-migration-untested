"""Per-object migration playbook helpers — pure SQL generation, no execution."""
from __future__ import annotations

from dataclasses import dataclass

from utils.discovery import ObjectRecord
from utils.paths import AdlsPath, S3Path, parse_storage_url
from utils.sql import quote_fqn, quote_ident


@dataclass(frozen=True)
class MigrationPlan:
    """An ordered list of (action_name, sql) tuples to execute for one object."""
    steps: list[tuple[str, str]]


def derive_pre_migration_fqn(catalog: str, schema: str, name: str) -> tuple[str, str, str]:
    return (catalog, schema, f"{name}__pre_migration")


def derive_staging_fqn(catalog: str, schema: str, name: str) -> tuple[str, str, str]:
    return (catalog, schema, f"{name}__migrate_staging")


def rewrite_account_in_path(path: str, new_account: str, *, old_account: str | None = None) -> str:
    """Rewrite the storage account (ADLS) or bucket (S3) in a storage URL.

    Container/path stay intact. The S3 scheme of the input (s3 / s3a / s3n)
    is normalized to `s3://` on output — Spark and UC accept all three, and
    keeping one canonical form avoids round-trip drift.

    Prefix-as-account mode (S3 only): if `new_account` contains a slash, it
    is interpreted as 'bucket/prefix'. The `old_account` (which must also
    contain a slash) is stripped from the front of the URL path and replaced
    with `new_account`. Use this for single-bucket S3 testing where OLD/NEW
    are sibling prefixes.
    """
    parsed = parse_storage_url(path)
    if parsed is None:
        raise ValueError(f"Not a recognized storage URL (abfss/s3): {path}")
    if isinstance(parsed, AdlsPath):
        suffix = f"/{parsed.path}" if parsed.path else ""
        return f"abfss://{parsed.container}@{new_account}.dfs.core.windows.net{suffix}"
    if isinstance(parsed, S3Path):
        # Prefix mode
        if "/" in new_account:
            if old_account is None or "/" not in old_account:
                raise ValueError(
                    "Prefix-mode rewrite requires both old_account and new_account "
                    "to be 'bucket/prefix' strings."
                )
            old_norm = old_account.rstrip("/").lower()
            new_norm = new_account.rstrip("/")
            canon = f"{parsed.account}/{parsed.path}".rstrip("/").lower()
            if canon == old_norm:
                return f"s3://{new_norm}"
            if canon.startswith(old_norm + "/"):
                tail = canon[len(old_norm) + 1:]
                # preserve original case of the tail
                # (URL was lowered for comparison; restore from parsed.path)
                tail_orig = f"{parsed.account}/{parsed.path}".rstrip("/")[len(old_norm) + 1:]
                return f"s3://{new_norm}/{tail_orig}"
            raise ValueError(
                f"URL {path} does not start with old_account prefix '{old_account}'"
            )
        # Bucket mode (no slash in new_account)
        suffix = f"/{parsed.path}" if parsed.path else ""
        return f"s3://{new_account}{suffix}"
    raise ValueError(f"Unknown storage path variant: {type(parsed).__name__}")


# Back-compat alias for callers that imported the prior name.
_rewrite_account = rewrite_account_in_path


def plan_managed_delta_migration(*, rec: ObjectRecord) -> MigrationPlan:
    """Delta managed table → DEEP CLONE staging, then two RENAMEs."""
    orig = quote_fqn(rec.catalog, rec.schema, rec.name)
    pre = quote_fqn(*derive_pre_migration_fqn(rec.catalog, rec.schema, rec.name))
    staging = quote_fqn(*derive_staging_fqn(rec.catalog, rec.schema, rec.name))
    return MigrationPlan(steps=[
        ("clone", f"CREATE TABLE {staging} DEEP CLONE {orig}"),
        ("rename_orig", f"ALTER TABLE {orig} RENAME TO {pre}"),
        ("rename_staging", f"ALTER TABLE {staging} RENAME TO {orig}"),
    ])


def plan_managed_non_delta_migration(*, rec: ObjectRecord) -> MigrationPlan:
    """Non-Delta managed table → CTAS staging, then two RENAMEs.

    Time-travel history is NOT preserved by CTAS. Caller should validate
    row count and schema after clone.
    """
    orig = quote_fqn(rec.catalog, rec.schema, rec.name)
    pre = quote_fqn(*derive_pre_migration_fqn(rec.catalog, rec.schema, rec.name))
    staging = quote_fqn(*derive_staging_fqn(rec.catalog, rec.schema, rec.name))
    return MigrationPlan(steps=[
        ("ctas", f"CREATE TABLE {staging} AS SELECT * FROM {orig}"),
        ("rename_orig", f"ALTER TABLE {orig} RENAME TO {pre}"),
        ("rename_staging", f"ALTER TABLE {staging} RENAME TO {orig}"),
    ])


def plan_external_table_migration(
    *, rec: ObjectRecord, new_storage_account: str,
    old_storage_account: str | None = None,
) -> MigrationPlan:
    """External table → DROP + CREATE EXTERNAL TABLE at new path.

    UC does not support ALTER TABLE SET LOCATION for external tables, so the
    only safe path is DROP+CREATE. Grants must be replayed afterward via
    GovernanceReplayer.

    `old_storage_account` is required when running in S3 prefix-mode (where
    new_storage_account contains a '/'). Ignored otherwise.
    """
    orig = quote_fqn(rec.catalog, rec.schema, rec.name)
    if not rec.storage_path:
        raise ValueError(f"External table {orig} has no storage_path")
    new_path = rewrite_account_in_path(
        rec.storage_path, new_storage_account, old_account=old_storage_account,
    )
    fmt = (rec.data_source_format or "DELTA").upper()
    return MigrationPlan(steps=[
        ("drop", f"DROP TABLE {orig}"),
        ("create", f"CREATE EXTERNAL TABLE {orig} USING {fmt} LOCATION '{new_path}'"),
    ])


def plan_external_volume_migration(
    *, rec: ObjectRecord, new_storage_account: str,
    old_storage_account: str | None = None,
) -> MigrationPlan:
    """External volume → DROP + CREATE EXTERNAL VOLUME at new path.

    No ALTER VOLUME SET LOCATION exists.

    `old_storage_account` is required when running in S3 prefix-mode.
    """
    orig = quote_fqn(rec.catalog, rec.schema, rec.name)
    if not rec.storage_path:
        raise ValueError(f"External volume {orig} has no storage_path")
    new_path = rewrite_account_in_path(
        rec.storage_path, new_storage_account, old_account=old_storage_account,
    )
    return MigrationPlan(steps=[
        ("drop", f"DROP VOLUME {orig}"),
        ("create", f"CREATE EXTERNAL VOLUME {orig} LOCATION '{new_path}'"),
    ])


# --- Managed volume migration (staging-swap; copy is imperative, done in 03b) ---
# Managed volumes have no DEEP CLONE and no ALTER VOLUME SET LOCATION. To move
# one to new storage we create a fresh managed volume (which lands on the
# schema's repointed managed location), physically copy the files into it,
# verify, then swap names (orig -> __pre_migration, staging -> orig). These are
# the pure SQL builders; the file copy + integrity verification live in the
# notebook because they need dbutils.fs.

def build_create_managed_volume_sql(catalog: str, schema: str, name: str) -> str:
    """A managed volume is created with no LOCATION; UC places it at the
    schema's managed location (repointed to new storage by 00_repoint_schemas)."""
    return f"CREATE VOLUME {quote_fqn(catalog, schema, name)}"


def build_drop_volume_sql(catalog: str, schema: str, name: str) -> str:
    return f"DROP VOLUME {quote_fqn(catalog, schema, name)}"


def build_rename_volume_sql(catalog: str, schema: str, name: str, new_name: str) -> str:
    """`new_name` is a bare volume name in the same schema (RENAME TO takes a
    name, not a fully-qualified path)."""
    return f"ALTER VOLUME {quote_fqn(catalog, schema, name)} RENAME TO {quote_ident(new_name)}"


def compare_volume_listings(old, new) -> tuple[bool, dict]:
    """Compare two recursive volume file listings for copy completeness.

    Each listing is an iterable of ``(relpath, size_bytes)``. A match requires
    the SAME set of relative paths AND identical sizes per path. This is
    file-level count+size+path integrity (not a per-file content hash); the
    storage layer checksums each object on write, and `dbutils.fs.cp` is a
    faithful copy, so count+size+path is a strong completeness gate.

    Returns ``(match, evidence)``.
    """
    old_map = {rp: sz for rp, sz in old}
    new_map = {rp: sz for rp, sz in new}
    old_keys, new_keys = set(old_map), set(new_map)
    missing = sorted(old_keys - new_keys)        # present in source, not copied
    extra = sorted(new_keys - old_keys)          # present in target, unexpected
    size_mismatch = sorted(p for p in (old_keys & new_keys) if old_map[p] != new_map[p])
    match = not missing and not extra and not size_mismatch
    evidence = {
        "old_file_count": len(old_map), "new_file_count": len(new_map),
        "old_total_bytes": sum(old_map.values()), "new_total_bytes": sum(new_map.values()),
        "missing": missing[:50], "extra": extra[:50], "size_mismatch": size_mismatch[:50],
        "match": match,
    }
    return match, evidence
