from unittest.mock import MagicMock

import pytest

from utils.preflight import (
    PreflightResult,
    check_external_location_for,
    probe_path_exists,
    probe_partition_completeness,
)
from utils.uc_client import ExternalLocationRecord


def test_check_external_location_for_returns_matching_root():
    ext_locs = [
        ExternalLocationRecord("old", "abfss://c@old.dfs.core.windows.net/", "cred1", False, "eastus"),
        ExternalLocationRecord("new", "abfss://c@new.dfs.core.windows.net/", "cred2", False, "eastus"),
    ]
    target = "abfss://c@new.dfs.core.windows.net/some/data/path"
    el = check_external_location_for(target_path=target, external_locations=ext_locs)
    assert el is not None
    assert el.name == "new"


def test_check_external_location_for_returns_none_when_no_match():
    ext_locs = [
        ExternalLocationRecord("old", "abfss://c@old.dfs.core.windows.net/", "cred1", False, "eastus"),
    ]
    target = "abfss://c@third.dfs.core.windows.net/x"
    assert check_external_location_for(target_path=target, external_locations=ext_locs) is None


def test_probe_path_exists_uses_dbutils_fs_ls():
    fs = MagicMock()
    fs.ls.return_value = [MagicMock()]
    assert probe_path_exists(fs=fs, path="abfss://c@new.dfs.core.windows.net/x") is True
    fs.ls.assert_called_with("abfss://c@new.dfs.core.windows.net/x")


def test_probe_path_exists_returns_false_on_exception():
    fs = MagicMock()
    fs.ls.side_effect = Exception("path not found")
    assert probe_path_exists(fs=fs, path="abfss://c@new.dfs.core.windows.net/x") is False


def test_probe_partition_completeness_counts_matching_directories():
    fs = MagicMock()
    def ls_side_effect(p):
        if "@old" in p:
            return [MagicMock() for _ in range(3)]
        if "@new" in p:
            return [MagicMock() for _ in range(3)]
        return []
    fs.ls.side_effect = ls_side_effect
    result = probe_partition_completeness(
        fs=fs,
        old_path="abfss://c@old.dfs.core.windows.net/x",
        new_path="abfss://c@new.dfs.core.windows.net/x",
    )
    assert result.old_count == 3
    assert result.new_count == 3
    assert result.complete is True


def test_probe_partition_completeness_detects_missing():
    fs = MagicMock()
    def ls_side_effect(p):
        if "@old" in p:
            return [MagicMock() for _ in range(5)]
        return [MagicMock() for _ in range(3)]
    fs.ls.side_effect = ls_side_effect
    result = probe_partition_completeness(
        fs=fs,
        old_path="abfss://c@old.dfs.core.windows.net/x",
        new_path="abfss://c@new.dfs.core.windows.net/x",
    )
    assert result.complete is False
    assert result.new_count == 3
    assert result.old_count == 5
