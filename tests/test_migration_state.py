import pytest

from utils.migration_state import (
    derive_object_state, plan_forward_resume, is_resumable, forward_worklist,
    PRISTINE, CLONED, SWAP_PARTIAL, SWAPPED, SWAPPED_ORPHAN, GAP, ONLY_STAGING, ABSENT,
    CLONE, RENAME_ORIG_TO_PRE, PROMOTE_STAGING, REPLAY, VALIDATE, DROP_ORPHAN_STAGING,
)


def D(o, p, s):
    return derive_object_state(orig_exists=o, pre_exists=p, staging_exists=s)


def test_state_derivation_full_matrix():
    assert D(True, False, False) == PRISTINE
    assert D(True, False, True) == CLONED
    assert D(False, True, True) == SWAP_PARTIAL
    assert D(True, True, False) == SWAPPED
    assert D(True, True, True) == SWAPPED_ORPHAN
    assert D(False, True, False) == GAP
    assert D(False, False, True) == ONLY_STAGING
    assert D(False, False, False) == ABSENT


def test_plan_pristine_runs_everything():
    assert plan_forward_resume(PRISTINE) == [
        CLONE, RENAME_ORIG_TO_PRE, PROMOTE_STAGING, REPLAY, VALIDATE]


def test_plan_cloned_skips_the_expensive_clone():
    # The whole point of resume-from-furthest-progress: never re-clone.
    assert CLONE not in plan_forward_resume(CLONED)
    assert plan_forward_resume(CLONED) == [
        RENAME_ORIG_TO_PRE, PROMOTE_STAGING, REPLAY, VALIDATE]


def test_plan_swap_partial_only_promotes():
    assert plan_forward_resume(SWAP_PARTIAL) == [PROMOTE_STAGING, REPLAY, VALIDATE]


def test_plan_swapped_only_replay_validate():
    assert plan_forward_resume(SWAPPED) == [REPLAY, VALIDATE]


def test_plan_swapped_orphan_drops_leftover_staging():
    assert plan_forward_resume(SWAPPED_ORPHAN) == [REPLAY, VALIDATE, DROP_ORPHAN_STAGING]


def test_unrecoverable_states_yield_no_plan_and_need_review():
    for state in (GAP, ONLY_STAGING, ABSENT):
        assert plan_forward_resume(state) == []
        assert is_resumable(state) is False


def test_resumable_states():
    for s in (PRISTINE, CLONED, SWAP_PARTIAL, SWAPPED, SWAPPED_ORPHAN):
        assert is_resumable(s) is True


def test_forward_worklist_excludes_validated_up_front():
    objs = [{"k": "a"}, {"k": "b"}, {"k": "c"}]
    out = forward_worklist(objs, {"a", "c"}, key=lambda o: o["k"])
    assert [o["k"] for o in out] == ["b"]
