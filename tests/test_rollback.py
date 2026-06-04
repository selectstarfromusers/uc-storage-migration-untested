import pytest

from utils.rollback import plan_rollback, NOOP, WARN, ERROR


def _labels(steps):
    return [s[0] for s in steps]


def _sql(steps):
    return " | ".join(s[1] for s in steps)


# ---- Managed table: full swap done (validated/replayed/swapped) ----
def test_managed_full_swap_restores_shadow():
    steps = plan_rollback(
        object_type="TABLE", table_type="MANAGED", catalog="c", schema="s", name="t",
        pre_fqn="`c`.`s`.`t__pre_migration`", staging_fqn="`c`.`s`.`t__migrate_staging`",
        orig_exists=True, pre_exists=True, staging_exists=False,
    )
    assert _sql(steps) == (
        "DROP TABLE IF EXISTS `c`.`s`.`t` | "
        "ALTER TABLE `c`.`s`.`t__pre_migration` RENAME TO `t`"
    )


# ---- Managed table: swap died BETWEEN the two renames (orig missing) ----
def test_managed_half_swap_orig_missing_restores_from_shadow():
    steps = plan_rollback(
        object_type="TABLE", table_type="MANAGED", catalog="c", schema="s", name="t",
        pre_fqn="`c`.`s`.`t__pre_migration`", staging_fqn="`c`.`s`.`t__migrate_staging`",
        orig_exists=False, pre_exists=True, staging_exists=True,
    )
    # No drop of orig (it's gone); rename shadow back; drop the orphan staging.
    assert _labels(steps) == ["restore shadow → orig", "drop orphan staging TABLE"]
    assert "ALTER TABLE `c`.`s`.`t__pre_migration` RENAME TO `t`" in _sql(steps)
    assert "DROP TABLE IF EXISTS `c`.`s`.`t__migrate_staging`" in _sql(steps)


# ---- Managed table: only cloned, not swapped (orig intact, staging orphan) ----
def test_managed_cloned_only_drops_orphan_staging():
    steps = plan_rollback(
        object_type="TABLE", table_type="MANAGED", catalog="c", schema="s", name="t",
        pre_fqn="`c`.`s`.`t__pre_migration`", staging_fqn="`c`.`s`.`t__migrate_staging`",
        orig_exists=True, pre_exists=False, staging_exists=True,
    )
    assert _labels(steps) == ["drop orphan staging TABLE"]
    assert "DROP TABLE IF EXISTS `c`.`s`.`t__migrate_staging`" in _sql(steps)


# ---- Managed table: snapshot_taken only (nothing changed) → no-op ----
def test_managed_nothing_done_is_noop():
    steps = plan_rollback(
        object_type="TABLE", table_type="MANAGED", catalog="c", schema="s", name="t",
        pre_fqn="`c`.`s`.`t__pre_migration`", staging_fqn="`c`.`s`.`t__migrate_staging`",
        orig_exists=True, pre_exists=False, staging_exists=False,
    )
    assert _labels(steps) == [NOOP]


# ---- Managed VOLUME: full swap uses VOLUME keyword (not external recreate) ----
def test_managed_volume_full_swap_uses_volume_rename():
    steps = plan_rollback(
        object_type="VOLUME", table_type="MANAGED", catalog="c", schema="s", name="v",
        pre_fqn="`c`.`s`.`v__pre_migration`", staging_fqn="`c`.`s`.`v__migrate_staging`",
        orig_exists=True, pre_exists=True, staging_exists=False,
    )
    assert _sql(steps) == (
        "DROP VOLUME IF EXISTS `c`.`s`.`v` | "
        "ALTER VOLUME `c`.`s`.`v__pre_migration` RENAME TO `v`"
    )


# ---- External table: drop + recreate at original path ----
def test_external_table_recreates_at_original_path():
    steps = plan_rollback(
        object_type="TABLE", table_type="EXTERNAL", catalog="c", schema="s", name="t",
        pre_fqn=None, staging_fqn=None,
        orig_exists=True, pre_exists=False, staging_exists=False,
        original_path="abfss://x@old.dfs.core.windows.net/t", data_source_format="delta",
    )
    assert _sql(steps) == (
        "DROP TABLE IF EXISTS `c`.`s`.`t` | "
        "CREATE EXTERNAL TABLE `c`.`s`.`t` USING DELTA LOCATION 'abfss://x@old.dfs.core.windows.net/t'"
    )


def test_external_table_orig_already_gone_only_recreates():
    steps = plan_rollback(
        object_type="TABLE", table_type="EXTERNAL", catalog="c", schema="s", name="t",
        pre_fqn=None, staging_fqn=None,
        orig_exists=False, pre_exists=False, staging_exists=False,
        original_path="s3://b/p", data_source_format="PARQUET",
    )
    assert _labels(steps) == ["recreate external table at old path"]
    assert "USING PARQUET LOCATION 's3://b/p'" in _sql(steps)


def test_external_volume_recreates_at_original_path():
    steps = plan_rollback(
        object_type="VOLUME", table_type="EXTERNAL", catalog="c", schema="s", name="v",
        pre_fqn=None, staging_fqn=None,
        orig_exists=True, pre_exists=False, staging_exists=False,
        original_path="s3://b/v",
    )
    assert "CREATE EXTERNAL VOLUME `c`.`s`.`v` LOCATION 's3://b/v'" in _sql(steps)


# ---- Error / warn edges ----
def test_external_without_original_path_errors():
    steps = plan_rollback(
        object_type="TABLE", table_type="EXTERNAL", catalog="c", schema="s", name="t",
        pre_fqn=None, staging_fqn=None,
        orig_exists=True, pre_exists=False, staging_exists=False, original_path=None,
    )
    assert _labels(steps) == [ERROR]


def test_everything_missing_errors():
    steps = plan_rollback(
        object_type="TABLE", table_type="MANAGED", catalog="c", schema="s", name="t",
        pre_fqn="`c`.`s`.`t__pre_migration`", staging_fqn="`c`.`s`.`t__migrate_staging`",
        orig_exists=False, pre_exists=False, staging_exists=False,
    )
    assert _labels(steps) == [ERROR]


def test_only_staging_left_warns_for_review():
    steps = plan_rollback(
        object_type="TABLE", table_type="MANAGED", catalog="c", schema="s", name="t",
        pre_fqn="`c`.`s`.`t__pre_migration`", staging_fqn="`c`.`s`.`t__migrate_staging`",
        orig_exists=False, pre_exists=False, staging_exists=True,
    )
    assert _labels(steps) == [WARN]
