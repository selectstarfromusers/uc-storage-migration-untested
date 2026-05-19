"""Per-object migration playbook helpers — pure SQL generation, no execution."""
from __future__ import annotations

from dataclasses import dataclass

from utils.discovery import ObjectRecord
from utils.paths import AdlsPath, S3Path, parse_storage_url
from utils.sql import quote_fqn


@dataclass(frozen=True)
class MigrationPlan:
    """An ordered list of (action_name, sql) tuples to execute for one object."""
    steps: list[tuple[str, str]]


def derive_pre_migration_fqn(catalog: str, schema: str, name: str) -> tuple[str, str, str]:
    return (catalog, schema, f"{name}__pre_migration")


def derive_staging_fqn(catalog: str, schema: str, name: str) -> tuple[str, str, str]:
    return (catalog, schema, f"{name}__migrate_staging")


def rewrite_account_in_path(path: str, new_account: str) -> str:
    """Rewrite the storage account (ADLS) or bucket (S3) in a storage URL.

    Container/path stay intact. The S3 scheme of the input (s3 / s3a / s3n)
    is normalized to `s3://` on output — Spark and UC accept all three, and
    keeping one canonical form avoids round-trip drift.
    """
    parsed = parse_storage_url(path)
    if parsed is None:
        raise ValueError(f"Not a recognized storage URL (abfss/s3): {path}")
    if isinstance(parsed, AdlsPath):
        suffix = f"/{parsed.path}" if parsed.path else ""
        return f"abfss://{parsed.container}@{new_account}.dfs.core.windows.net{suffix}"
    if isinstance(parsed, S3Path):
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
) -> MigrationPlan:
    """External table → DROP + CREATE EXTERNAL TABLE at new path.

    UC does not support ALTER TABLE SET LOCATION for external tables, so the
    only safe path is DROP+CREATE. Grants must be replayed afterward via
    GovernanceReplayer.
    """
    orig = quote_fqn(rec.catalog, rec.schema, rec.name)
    if not rec.storage_path:
        raise ValueError(f"External table {orig} has no storage_path")
    new_path = rewrite_account_in_path(rec.storage_path, new_storage_account)
    fmt = (rec.data_source_format or "DELTA").upper()
    return MigrationPlan(steps=[
        ("drop", f"DROP TABLE {orig}"),
        ("create", f"CREATE EXTERNAL TABLE {orig} USING {fmt} LOCATION '{new_path}'"),
    ])


def plan_external_volume_migration(
    *, rec: ObjectRecord, new_storage_account: str,
) -> MigrationPlan:
    """External volume → DROP + CREATE EXTERNAL VOLUME at new path.

    No ALTER VOLUME SET LOCATION exists.
    """
    orig = quote_fqn(rec.catalog, rec.schema, rec.name)
    if not rec.storage_path:
        raise ValueError(f"External volume {orig} has no storage_path")
    new_path = rewrite_account_in_path(rec.storage_path, new_storage_account)
    return MigrationPlan(steps=[
        ("drop", f"DROP VOLUME {orig}"),
        ("create", f"CREATE EXTERNAL VOLUME {orig} LOCATION '{new_path}'"),
    ])
