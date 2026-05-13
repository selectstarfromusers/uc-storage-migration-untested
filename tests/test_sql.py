import pytest
from utils.sql import quote_ident, quote_fqn, parse_describe_extended_location


class TestQuoteIdent:
    def test_simple_identifier(self):
        assert quote_ident("my_table") == "`my_table`"

    def test_identifier_with_backtick_escapes(self):
        assert quote_ident("weird`name") == "`weird``name`"

    def test_identifier_with_space(self):
        assert quote_ident("with space") == "`with space`"


class TestQuoteFqn:
    def test_three_part(self):
        assert quote_fqn("catalog", "schema", "table") == "`catalog`.`schema`.`table`"

    def test_with_special_chars(self):
        assert quote_fqn("c-1", "s.s", "t`t") == "`c-1`.`s.s`.`t``t`"

    def test_two_part(self):
        assert quote_fqn("catalog", "schema") == "`catalog`.`schema`"


class TestParseDescribeExtendedLocation:
    def test_extracts_location_from_block(self):
        output = """
col_name             data_type            comment
id                   bigint
name                 string

# Detailed Table Information
Catalog              main
Database             schema_a
Table                t1
Location             abfss://c@oldacct.dfs.core.windows.net/managed/x
Provider             delta
"""
        assert parse_describe_extended_location(output) == "abfss://c@oldacct.dfs.core.windows.net/managed/x"

    def test_returns_none_when_no_location(self):
        assert parse_describe_extended_location("no location line here") is None

    def test_handles_tab_separated(self):
        output = "Location\tabfss://c@a.dfs.core.windows.net/p"
        assert parse_describe_extended_location(output) == "abfss://c@a.dfs.core.windows.net/p"
