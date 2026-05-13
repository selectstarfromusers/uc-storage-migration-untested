import pytest
from utils.paths import parse_abfss_url, classify_account, AdlsPath


class TestParseAbfssUrl:
    def test_parses_standard_url(self):
        result = parse_abfss_url("abfss://container@oldacct.dfs.core.windows.net/some/path")
        assert result == AdlsPath(
            account="oldacct",
            container="container",
            path="some/path",
            raw="abfss://container@oldacct.dfs.core.windows.net/some/path",
        )

    def test_parses_url_with_trailing_slash(self):
        result = parse_abfss_url("abfss://c@a.dfs.core.windows.net/")
        assert result.account == "a"
        assert result.container == "c"
        assert result.path == ""

    def test_returns_none_for_non_abfss(self):
        assert parse_abfss_url("s3://bucket/path") is None
        assert parse_abfss_url("/Volumes/c/s/v/file") is None
        assert parse_abfss_url(None) is None
        assert parse_abfss_url("") is None

    def test_handles_uppercase_host(self):
        result = parse_abfss_url("abfss://c@MyAcct.dfs.core.windows.net/x")
        assert result.account == "myacct"  # normalized to lowercase


class TestClassifyAccount:
    def test_old_account(self):
        assert classify_account("oldacct", old="oldacct", new="newacct") == "old"

    def test_new_account(self):
        assert classify_account("newacct", old="oldacct", new="newacct") == "new"

    def test_other_account(self):
        assert classify_account("thirdparty", old="oldacct", new="newacct") == "other"

    def test_none_is_unknown(self):
        assert classify_account(None, old="oldacct", new="newacct") == "unknown"

    def test_case_insensitive(self):
        assert classify_account("OldAcct", old="oldacct", new="newacct") == "old"
