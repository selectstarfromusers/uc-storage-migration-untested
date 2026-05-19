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


def test_parse_show_grants_rows_handles_uc_capitalized_columns():
    """UC's SHOW GRANTS returns 'Principal', 'ActionType', 'ObjectType' — not lowercase."""
    rows = [
        {"Principal": "alice@x.com", "ActionType": "SELECT", "ObjectType": "TABLE", "ObjectKey": "c.s.t"},
        {"Principal": "bob@x.com", "ActionType": "ALL PRIVILEGES", "ObjectType": "CATALOG", "ObjectKey": "c"},
    ]
    result = parse_show_grants_rows(rows)
    assert result == [
        GrantEntry(principal="alice@x.com", privilege="SELECT", object_type="TABLE"),
        GrantEntry(principal="bob@x.com", privilege="ALL PRIVILEGES", object_type="CATALOG"),
    ]


def test_parse_show_grants_rows_skips_rows_with_no_principal():
    rows = [{"ActionType": "SELECT"}]  # malformed — no principal
    assert parse_show_grants_rows(rows) == []


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


from unittest.mock import MagicMock

from utils.governance import GovernanceCapturer, GovernanceReplayer


class _Row(dict):
    def asDict(self):
        return dict(self)


def _spark_returning(*calls):
    """Return a spark mock where consecutive .sql(...).collect() calls yield each call's rows."""
    spark = MagicMock()
    results = []
    for call in calls:
        r = MagicMock()
        r.collect.return_value = call
        results.append(r)
    spark.sql.side_effect = results
    return spark


class TestGovernanceCapturer:
    def test_capture_assembles_snapshot(self):
        spark = _spark_returning(
            [_Row(principal="u1", action_type="SELECT", object_type="TABLE", object_key="c.s.t")],
            [_Row(col_name="Owner", data_type="alice")],
            [_Row(tag_name="pii", tag_value="true")],
            [],
            [],
            [_Row(col_name="id", data_type="bigint", comment="primary key")],
            [_Row(key="delta.columnMapping.mode", value="name")],
        )
        cap = GovernanceCapturer(spark=spark)

        snap = cap.capture(catalog="c", schema="s", name="t")

        assert snap.grants == [GrantEntry("u1", "SELECT", "TABLE")]
        assert snap.owner == "alice"
        assert snap.tags == [TagEntry("pii", "true")]
        assert snap.column_comments == {"id": "primary key"}
        assert snap.table_properties == {"delta.columnMapping.mode": "name"}


class TestGovernanceReplayer:
    def test_replay_emits_grant_owner_tag_comment(self):
        spark = MagicMock()
        rep = GovernanceReplayer(spark=spark)
        snap = GovernanceSnapshot(
            catalog="c", schema="s", name="t",
            grants=[GrantEntry("u1", "SELECT", "TABLE")],
            owner="alice",
            tags=[TagEntry("pii", "true")],
            row_filter_name=None, row_filter_using_columns=[],
            column_masks=[],
            table_comment="hello",
            column_comments={},
            table_properties={},
        )

        warnings = rep.replay(snap, target_fqn=("c", "s", "t"))

        executed = [call.args[0] for call in spark.sql.call_args_list]
        assert "GRANT SELECT ON TABLE `c`.`s`.`t` TO `u1`" in executed
        assert "ALTER TABLE `c`.`s`.`t` OWNER TO `alice`" in executed
        assert any("SET TAGS" in s for s in executed)
        assert any("COMMENT ON TABLE" in s for s in executed)
        assert warnings == []

    def test_replay_warns_on_row_filter(self):
        spark = MagicMock()
        snap = GovernanceSnapshot(
            catalog="c", schema="s", name="t",
            grants=[], owner=None, tags=[],
            row_filter_name="filter_x", row_filter_using_columns=["col1"],
            column_masks=[],
            table_comment=None, column_comments={}, table_properties={},
        )
        warnings = GovernanceReplayer(spark=spark).replay(snap, target_fqn=("c", "s", "t"))
        assert any("row filter 'filter_x'" in w for w in warnings)

    def test_replay_collects_grant_failure(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("principal not found")
        snap = GovernanceSnapshot(
            catalog="c", schema="s", name="t",
            grants=[GrantEntry("u_deleted", "SELECT", "TABLE")],
            owner=None, tags=[], row_filter_name=None, row_filter_using_columns=[],
            column_masks=[], table_comment=None, column_comments={}, table_properties={},
        )
        warnings = GovernanceReplayer(spark=spark).replay(snap, target_fqn=("c", "s", "t"))
        assert any("grant replay failed" in w for w in warnings)
