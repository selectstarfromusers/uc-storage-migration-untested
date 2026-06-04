"""State-aware rollback planning.

`03b_forward_migrate` can stop at any point (snapshot_taken, cloned, swapped,
replayed, validated, or failed mid-step). A rollback that trusts only the logged
status is fragile. Instead we inspect the ACTUAL current existence of the three
objects a managed migration juggles —

  - ``orig``    : the live object at its real FQN
  - ``pre``     : the ``<name>__pre_migration`` shadow (the original, post-swap)
  - ``staging`` : the ``<name>__migrate_staging`` clone (pre-swap)

— and emit idempotent SQL to reconcile back to the pre-migration state from
whatever point the migration reached. The desired end state is always: ``orig``
exists and equals the original; no ``pre``; no ``staging``.

This module is pure (no spark/dbutils); the notebook probes existence and runs
the returned statements.
"""
from __future__ import annotations

from utils.sql import quote_fqn


# Sentinel labels the notebook special-cases (no SQL to run):
NOOP = "__NOOP__"      # nothing migrated / already in desired state
WARN = "__WARN__"      # ambiguous partial state — surface for manual review
ERROR = "__ERROR__"    # cannot roll back from here


def _kw(object_type: str) -> str:
    return "VOLUME" if (object_type or "").upper() == "VOLUME" else "TABLE"


def plan_rollback(
    *,
    object_type: str,
    table_type: str | None,
    catalog: str,
    schema: str,
    name: str,
    pre_fqn: str | None,
    staging_fqn: str | None,
    orig_exists: bool,
    pre_exists: bool,
    staging_exists: bool,
    original_path: str | None = None,
    data_source_format: str | None = "DELTA",
) -> list[tuple[str, str]]:
    """Return idempotent ``(label, sql)`` steps to restore one object.

    Existence-driven, so it works from ANY point 03b stopped at. `pre_fqn` /
    `staging_fqn` are the (possibly derived) shadow/staging FQNs; the matching
    `*_exists` flags say whether they currently exist in UC. For external
    objects (no shadow) `original_path` is the pre-migration storage location.
    """
    kw = _kw(object_type)
    orig_fqn = quote_fqn(catalog, schema, name)
    is_external = (table_type or "").upper() == "EXTERNAL"

    # --- External: 03b did DROP orig + CREATE ... at the NEW path (no shadow).
    # Restore = (re)create at the ORIGINAL path. Idempotent via DROP IF EXISTS.
    if is_external:
        if not original_path:
            return [(ERROR, f"{orig_fqn}: external object but no original_storage_path; cannot roll back")]
        steps: list[tuple[str, str]] = []
        if orig_exists:
            steps.append((f"drop migrated {kw}", f"DROP {kw} IF EXISTS {orig_fqn}"))
        if kw == "VOLUME":
            steps.append(("recreate external volume at old path",
                          f"CREATE EXTERNAL VOLUME {orig_fqn} LOCATION '{original_path}'"))
        else:
            fmt = (data_source_format or "DELTA").upper()
            steps.append(("recreate external table at old path",
                          f"CREATE EXTERNAL TABLE {orig_fqn} USING {fmt} LOCATION '{original_path}'"))
        return steps

    # --- Managed table OR managed volume: the staging-swap pattern.
    steps = []
    if pre_exists:
        # The original lives in the shadow → whatever sits at orig now is the
        # migrated copy (full case) or nothing (swap died between the two
        # renames). Either way: drop migrated-if-present, rename shadow back.
        if orig_exists:
            steps.append((f"drop migrated {kw}", f"DROP {kw} IF EXISTS {orig_fqn}"))
        # RENAME target must be fully qualified — a bare name resolves against
        # the session schema (CANNOT_RENAME_ACROSS_SCHEMA), esp. for volumes.
        steps.append(("restore shadow → orig", f"ALTER {kw} {pre_fqn} RENAME TO {orig_fqn}"))
        if staging_exists:
            steps.append((f"drop orphan staging {kw}", f"DROP {kw} IF EXISTS {staging_fqn}"))
        return steps

    # No shadow → the original was never renamed; orig (if present) IS original.
    if orig_exists:
        if staging_exists:
            # Partial clone (status ~ cloned): original intact, staging orphaned.
            return [(f"drop orphan staging {kw}", f"DROP {kw} IF EXISTS {staging_fqn}")]
        return [(NOOP, f"{orig_fqn}: original intact, nothing to undo")]

    # orig missing AND no shadow.
    if staging_exists:
        return [(WARN, f"{orig_fqn}: original + shadow both missing; only staging "
                       f"{staging_fqn} exists — manual review (data may be recoverable from staging)")]
    return [(ERROR, f"{orig_fqn}: original, shadow, and staging all missing — cannot roll back from this repo")]
