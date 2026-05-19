import pytest
from utils.paths import (
    parse_abfss_url, parse_s3_url, parse_storage_url, classify_account,
    AdlsPath, S3Path,
)


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


class TestParseS3Url:
    def test_parses_basic_s3(self):
        result = parse_s3_url("s3://mybucket/some/key")
        assert result == S3Path(
            account="mybucket",
            container="",
            path="some/key",
            raw="s3://mybucket/some/key",
        )

    def test_parses_bucket_only(self):
        result = parse_s3_url("s3://mybucket/")
        assert result.account == "mybucket"
        assert result.path == ""

    def test_parses_bucket_with_no_trailing_slash(self):
        result = parse_s3_url("s3://mybucket")
        assert result.account == "mybucket"
        assert result.path == ""

    def test_parses_s3a_variant(self):
        result = parse_s3_url("s3a://mybucket/some/key")
        assert result.account == "mybucket"

    def test_parses_s3n_variant(self):
        result = parse_s3_url("s3n://mybucket/some/key")
        assert result.account == "mybucket"

    def test_returns_none_for_non_s3(self):
        assert parse_s3_url("abfss://c@a.dfs.core.windows.net/x") is None
        assert parse_s3_url("/Volumes/c/s/v/file") is None
        assert parse_s3_url(None) is None
        assert parse_s3_url("") is None

    def test_normalizes_bucket_to_lowercase(self):
        result = parse_s3_url("s3://MyBucket/x")
        assert result.account == "mybucket"


class TestParseStorageUrl:
    def test_dispatches_to_abfss(self):
        r = parse_storage_url("abfss://c@a.dfs.core.windows.net/x")
        assert isinstance(r, AdlsPath)
        assert r.account == "a"
        assert r.scheme == "abfss"

    def test_dispatches_to_s3(self):
        r = parse_storage_url("s3://mybucket/x")
        assert isinstance(r, S3Path)
        assert r.account == "mybucket"
        assert r.scheme == "s3"

    def test_returns_none_for_unknown(self):
        assert parse_storage_url("file:///tmp/x") is None
        assert parse_storage_url("/Volumes/c/s/v/file") is None
        assert parse_storage_url(None) is None
