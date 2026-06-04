"""Shared object-state model for resume + rollback.

A managed object mid-migration involves three names — the live `orig`, the
`__pre_migration` shadow, and the `__migrate_staging` clone. Both the forward
resume path and the rollback path need to reason about "given what currently
EXISTS, where are we, and what's left?" This module derives that state ONCE so
forward and rollback can't disagree.

Pure (no spark/dbutils) → fully unit-tested. Callers probe existence, derive the
state here, then execute the planned phases.
"""
from __future__ import annotations

# Object states for the staging-swap (managed table/volume) pattern.
PRISTINE = "pristine"            # orig only — not started (or done + cleaned)
CLONED = "cloned"               # orig + staging — clone done, swap not started
SWAP_PARTIAL = "swap_partial"   # pre + staging, no orig — first rename done, promote not
SWAPPED = "swapped"             # orig + pre — both renames done; replay/validate may remain
SWAPPED_ORPHAN = "swapped_orphan"  # orig + pre + staging — promoted but staging left behind
GAP = "gap"                     # pre only — orig renamed away, no staging to promote (broken)
ONLY_STAGING = "only_staging"   # staging only — orig + shadow both gone (pathological)
ABSENT = "absent"               # nothing exists — object is gone

# Forward phases (ordered) the executor knows how to run.
CLONE = "clone"
RENAME_ORIG_TO_PRE = "rename_orig_to_pre"
PROMOTE_STAGING = "promote_staging"
REPLAY = "replay"
VALIDATE = "validate"
DROP_ORPHAN_STAGING = "drop_orphan_staging"

# States from which forward cannot safely auto-resume — surface for review.
NEEDS_REVIEW = frozenset({GAP, ONLY_STAGING, ABSENT})


def derive_object_state(*, orig_exists: bool, pre_exists: bool, staging_exists: bool) -> str:
    """Classify a managed object's current migration state from existence."""
    if pre_exists and orig_exists:
        return SWAPPED_ORPHAN if staging_exists else SWAPPED
    if pre_exists and not orig_exists:
        return SWAP_PARTIAL if staging_exists else GAP
    # no pre:
    if orig_exists:
        return CLONED if staging_exists else PRISTINE
    return ONLY_STAGING if staging_exists else ABSENT


def plan_forward_resume(state: str) -> list[str]:
    """Phases still required to finish a forward migration from `state`.

    Returns [] for terminal/unrecoverable states (PRISTINE-done is disambiguated
    by the caller via migration_log status; NEEDS_REVIEW states return []). The
    executor runs the listed phases in order — so a resume never repeats the
    expensive CLONE once staging already exists.
    """
    if state == PRISTINE:
        return [CLONE, RENAME_ORIG_TO_PRE, PROMOTE_STAGING, REPLAY, VALIDATE]
    if state == CLONED:
        # staging already exists → skip the (expensive) clone.
        return [RENAME_ORIG_TO_PRE, PROMOTE_STAGING, REPLAY, VALIDATE]
    if state == SWAP_PARTIAL:
        # orig already renamed to pre → just promote staging.
        return [PROMOTE_STAGING, REPLAY, VALIDATE]
    if state == SWAPPED:
        return [REPLAY, VALIDATE]
    if state == SWAPPED_ORPHAN:
        return [REPLAY, VALIDATE, DROP_ORPHAN_STAGING]
    # GAP / ONLY_STAGING / ABSENT — not safely auto-resumable.
    return []


def is_resumable(state: str) -> bool:
    return state not in NEEDS_REVIEW


def forward_worklist(objects, validated_keys, *, key):
    """Drop already-validated objects from the work list up front (scale win:
    don't even visit the done ones). `key(obj)` → a hashable identity;
    `validated_keys` is the set of keys with migration_log status='validated'."""
    return [o for o in objects if key(o) not in validated_keys]
