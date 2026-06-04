"""Cleanup safety: only ever drop genuine `__pre_migration` shadows.

`05_cleanup` drops the shadow objects `03b` left behind. That is destructive and
(for volumes) irreversible, so every target is validated against a strict
convention BEFORE anything is dropped: it must be a fully-qualified 3-part name
whose object name ends with `__pre_migration`, and it must not live in the
OPS_SCHEMA. Anything that doesn't match is REJECTED, and the notebook refuses to
drop *anything* when there is even one rejection — fail closed, never guess.

Pure module (no spark/dbutils) so the guard logic is unit-tested.
"""
from __future__ import annotations

from utils.sql import quote_fqn

PRE_SUFFIX = "__pre_migration"


def _parts(fqn: str) -> list[str]:
    return [p for p in (fqn or "").replace("`", "").split(".")]


def derive_live_fqn(pre_migration_fqn: str) -> str | None:
    """Given a `<name>__pre_migration` shadow FQN, return the live original FQN
    (`<name>`), or None if the input isn't a well-formed shadow name."""
    parts = _parts(pre_migration_fqn)
    if len(parts) != 3 or not all(parts) or not parts[2].endswith(PRE_SUFFIX):
        return None
    cat, sch, name = parts
    live = name[: -len(PRE_SUFFIX)]
    if not live:
        return None
    return quote_fqn(cat, sch, live)


def validate_cleanup_targets(targets, *, ops_schema: str | None):
    """Partition cleanup targets into (accepted, rejected).

    `targets` is an iterable of ``(fqn, object_type)``. A target is ACCEPTED only
    if all hold:
      - fqn is a non-empty 3-part `catalog.schema.name`
      - the name component ends with ``__pre_migration``
      - it is not an object in OPS_SCHEMA
      - object_type is TABLE or VOLUME

    Returns ``(accepted, rejected)`` where accepted items are
    ``{"fqn", "object_type", "live_fqn"}`` and rejected are ``(fqn, reason)``.
    The caller MUST refuse to drop anything if `rejected` is non-empty.
    """
    ops_parts = _parts(ops_schema) if ops_schema else []
    ops_cat = ops_parts[0] if len(ops_parts) >= 1 else None
    ops_sch = ops_parts[1] if len(ops_parts) >= 2 else None

    accepted: list[dict] = []
    rejected: list[tuple[str, str]] = []
    for fqn, otype in targets:
        kind = (otype or "").upper()
        if not fqn or not isinstance(fqn, str):
            rejected.append((str(fqn), "empty or non-string fqn"))
            continue
        parts = _parts(fqn)
        if len(parts) != 3 or not all(parts):
            rejected.append((fqn, "not a 3-part catalog.schema.name"))
            continue
        cat, sch, name = parts
        if not name.endswith(PRE_SUFFIX):
            rejected.append((fqn, f"name does not end with '{PRE_SUFFIX}' — refusing (possible live object)"))
            continue
        if kind not in ("TABLE", "VOLUME"):
            rejected.append((fqn, f"unsupported object_type {otype!r}"))
            continue
        if ops_cat and ops_sch and cat == ops_cat and sch == ops_sch:
            rejected.append((fqn, "target is in OPS_SCHEMA — refusing"))
            continue
        accepted.append({"fqn": quote_fqn(cat, sch, name), "object_type": kind,
                         "live_fqn": derive_live_fqn(fqn)})
    return accepted, rejected


def build_drop_sql(fqn: str, object_type: str) -> str:
    kw = "VOLUME" if (object_type or "").upper() == "VOLUME" else "TABLE"
    return f"DROP {kw} IF EXISTS {fqn}"


def build_undrop_sql(fqn: str) -> str:
    """UNDROP applies to managed TABLES only (within the retention window).
    Volumes have no UNDROP — see can_undrop()."""
    return f"UNDROP TABLE {fqn}"


def can_undrop(object_type: str) -> bool:
    return (object_type or "").upper() == "TABLE"
