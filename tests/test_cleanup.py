import pytest

from utils.cleanup import (
    validate_cleanup_targets, derive_live_fqn, build_drop_sql, build_undrop_sql,
    can_undrop, PRE_SUFFIX,
)


def test_derive_live_fqn_strips_suffix():
    assert derive_live_fqn("c.s.orders__pre_migration") == "`c`.`s`.`orders`"
    assert derive_live_fqn("`c`.`s`.`orders__pre_migration`") == "`c`.`s`.`orders`"


def test_derive_live_fqn_rejects_non_shadow():
    assert derive_live_fqn("c.s.orders") is None          # no suffix
    assert derive_live_fqn("c.s") is None                 # not 3-part
    assert derive_live_fqn("c.s.__pre_migration") is None  # empty live name


def test_accepts_well_formed_shadow():
    accepted, rejected = validate_cleanup_targets(
        [("c.s.orders__pre_migration", "TABLE"), ("c.s.assets__pre_migration", "VOLUME")],
        ops_schema="c._migration_ops",
    )
    assert rejected == []
    assert {a["object_type"] for a in accepted} == {"TABLE", "VOLUME"}
    assert accepted[0]["live_fqn"] == "`c`.`s`.`orders`"


def test_rejects_non_shadow_name():
    # A live table name (no __pre_migration) must be REJECTED, not dropped.
    accepted, rejected = validate_cleanup_targets([("c.s.orders", "TABLE")], ops_schema="c._migration_ops")
    assert accepted == []
    assert len(rejected) == 1 and "does not end with" in rejected[0][1]


def test_rejects_ops_schema_object():
    accepted, rejected = validate_cleanup_targets(
        [("c._migration_ops.migration_log__pre_migration", "TABLE")], ops_schema="c._migration_ops")
    assert accepted == []
    assert "OPS_SCHEMA" in rejected[0][1]


def test_rejects_malformed_and_empty():
    accepted, rejected = validate_cleanup_targets(
        [("", "TABLE"), ("c.s", "TABLE"), (None, "TABLE"), ("c.s.x__pre_migration", "MODEL")],
        ops_schema="c._migration_ops")
    assert accepted == []
    assert len(rejected) == 4


def test_drop_sql_keyword_by_type():
    assert build_drop_sql("`c`.`s`.`t__pre_migration`", "TABLE") == "DROP TABLE IF EXISTS `c`.`s`.`t__pre_migration`"
    assert build_drop_sql("`c`.`s`.`v__pre_migration`", "VOLUME") == "DROP VOLUME IF EXISTS `c`.`s`.`v__pre_migration`"


def test_undrop_only_tables():
    assert can_undrop("TABLE") is True
    assert can_undrop("VOLUME") is False
    assert build_undrop_sql("`c`.`s`.`t__pre_migration`") == "UNDROP TABLE `c`.`s`.`t__pre_migration`"
