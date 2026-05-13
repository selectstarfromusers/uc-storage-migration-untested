import pytest

from utils.governance import (
    GovernanceSnapshot,
    GrantEntry,
    TagEntry,
    ColumnMaskEntry,
    build_show_grants_sql,
    build_show_tags_sql,
    build_show_row_filter_sql,
    parse_show_grants_rows,
    parse_show_tags_rows,
    build_replay_grants_sql,
    build_replay_owner_sql,
    build_replay_tags_sql,
    build_replay_comment_sql,
)


def test_build_show_grants_sql():
    sql = build_show_grants_sql(catalog="c", schema="s", name="t")
    assert sql == "SHOW GRANTS ON TABLE `c`.`s`.`t`"


def test_build_show_grants_sql_for_volume():
    sql = build_show_grants_sql(catalog="c", schema="s", name="v", object_type="VOLUME")
    assert sql == "SHOW GRANTS ON VOLUME `c`.`s`.`v`"


def test_build_show_tags_sql():
    sql = build_show_tags_sql(catalog="c", schema="s", name="t")
    assert "system.information_schema.table_tags" in sql


def test_build_show_row_filter_sql():
    sql = build_show_row_filter_sql(catalog="c", schema="s", name="t")
    assert "SHOW ROW FILTER" in sql
    assert "`c`.`s`.`t`" in sql


def test_parse_show_grants_rows():
    rows = [
        {"principal": "user1", "action_type": "SELECT", "object_type": "TABLE", "object_key": "c.s.t"},
        {"principal": "group1", "action_type": "MODIFY", "object_type": "TABLE", "object_key": "c.s.t"},
    ]
    result = parse_show_grants_rows(rows)
    assert result == [
        GrantEntry(principal="user1", privilege="SELECT", object_type="TABLE"),
        GrantEntry(principal="group1", privilege="MODIFY", object_type="TABLE"),
    ]


def test_parse_show_tags_rows():
    rows = [
        {"tag_name": "owner_team", "tag_value": "platform"},
        {"tag_name": "pii", "tag_value": "true"},
    ]
    result = parse_show_tags_rows(rows)
    assert result == [
        TagEntry(name="owner_team", value="platform"),
        TagEntry(name="pii", value="true"),
    ]


def test_build_replay_grants_sql_emits_one_grant_per_entry():
    grants = [
        GrantEntry(principal="u1", privilege="SELECT", object_type="TABLE"),
        GrantEntry(principal="g1", privilege="MODIFY", object_type="TABLE"),
    ]
    sqls = build_replay_grants_sql(catalog="c", schema="s", name="t", grants=grants)
    assert len(sqls) == 2
    assert sqls[0] == "GRANT SELECT ON TABLE `c`.`s`.`t` TO `u1`"
    assert sqls[1] == "GRANT MODIFY ON TABLE `c`.`s`.`t` TO `g1`"


def test_build_replay_owner_sql():
    sql = build_replay_owner_sql(catalog="c", schema="s", name="t", owner="alice@example.com")
    assert sql == "ALTER TABLE `c`.`s`.`t` OWNER TO `alice@example.com`"


def test_build_replay_tags_sql():
    tags = [TagEntry(name="pii", value="true"), TagEntry(name="owner", value="data")]
    sql = build_replay_tags_sql(catalog="c", schema="s", name="t", tags=tags)
    assert sql == "ALTER TABLE `c`.`s`.`t` SET TAGS ('pii' = 'true', 'owner' = 'data')"


def test_build_replay_tags_sql_empty():
    sql = build_replay_tags_sql(catalog="c", schema="s", name="t", tags=[])
    assert sql is None


def test_build_replay_comment_sql():
    sql = build_replay_comment_sql(catalog="c", schema="s", name="t", comment="my comment")
    assert sql == "COMMENT ON TABLE `c`.`s`.`t` IS 'my comment'"


def test_build_replay_comment_sql_escapes_quotes():
    sql = build_replay_comment_sql(catalog="c", schema="s", name="t", comment="it's good")
    assert "it''s good" in sql


def test_governance_snapshot_dataclass_holds_everything():
    snap = GovernanceSnapshot(
        catalog="c", schema="s", name="t",
        grants=[GrantEntry("u", "SELECT", "TABLE")],
        owner="alice",
        tags=[TagEntry("pii", "true")],
        row_filter_name=None,
        row_filter_using_columns=[],
        column_masks=[ColumnMaskEntry(column="ssn", mask_function="mask_ssn", using_columns=[])],
        table_comment="x",
        column_comments={"ssn": "Social Security Number"},
        table_properties={"delta.appendOnly": "true"},
    )
    assert snap.owner == "alice"
    assert len(snap.column_masks) == 1
    assert snap.column_masks[0].column == "ssn"
