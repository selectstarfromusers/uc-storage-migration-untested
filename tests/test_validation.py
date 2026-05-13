from unittest.mock import MagicMock

import pytest

from utils.validation import (
    EvidenceLayer,
    ValidationResult,
    validate_object_on_new,
    _parse_input_file_name_rows,
    _hosts_in_paths,
)


def test_hosts_in_paths_extracts_account():
    paths = [
        "abfss://c@newacct.dfs.core.windows.net/x/part-0.parquet",
        "abfss://c@newacct.dfs.core.windows.net/x/part-1.parquet",
    ]
    assert _hosts_in_paths(paths) == {"newacct"}


def test_hosts_in_paths_detects_mixed_hosts():
    paths = [
        "abfss://c@newacct.dfs.core.windows.net/x/p0",
        "abfss://c@oldacct.dfs.core.windows.net/x/p1",
    ]
    assert _hosts_in_paths(paths) == {"newacct", "oldacct"}


def test_validate_object_on_new_all_layers_pass():
    spark = MagicMock()
    fs = MagicMock()

    describe_rows = MagicMock()
    describe_rows.collect.return_value = [
        type("R", (), {"asDict": lambda self: {"col_name": "Location",
            "data_type": "abfss://c@newacct.dfs.core.windows.net/x"}})(),
    ]
    input_rows = MagicMock()
    input_rows.collect.return_value = [
        type("R", (), {"asDict": lambda self: {"path": "abfss://c@newacct.dfs.core.windows.net/x/p0.parquet"}})(),
    ]
    spark.sql.side_effect = [describe_rows, input_rows]
    fs.ls.return_value = [MagicMock()]

    result = validate_object_on_new(
        spark=spark, fs=fs,
        catalog="c", schema="s", name="t",
        expected_new_account="newacct",
        parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        is_delta=True,
    )

    assert result.overall_pass is True
    assert result.metadata_location_ok is True
    assert result.delta_log_at_new_ok is True
    assert result.input_file_name_ok is True
    assert result.parent_managed_location_match is True


def test_validate_object_on_new_input_file_on_old_fails():
    spark = MagicMock()
    describe_rows = MagicMock()
    describe_rows.collect.return_value = [
        type("R", (), {"asDict": lambda self: {"col_name": "Location",
            "data_type": "abfss://c@newacct.dfs.core.windows.net/x"}})(),
    ]
    input_rows = MagicMock()
    input_rows.collect.return_value = [
        type("R", (), {"asDict": lambda self: {"path": "abfss://c@oldacct.dfs.core.windows.net/x/p0.parquet"}})(),
    ]
    spark.sql.side_effect = [describe_rows, input_rows]
    fs = MagicMock()
    fs.ls.return_value = [MagicMock()]

    result = validate_object_on_new(
        spark=spark, fs=fs,
        catalog="c", schema="s", name="t",
        expected_new_account="newacct",
        parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        is_delta=True,
    )

    assert result.input_file_name_ok is False
    assert result.overall_pass is False


def test_parse_input_file_name_rows():
    rows = [
        type("R", (), {"asDict": lambda self: {"path": "abfss://c@n.dfs.core.windows.net/x/p1"}})(),
        type("R", (), {"asDict": lambda self: {"path": "abfss://c@n.dfs.core.windows.net/x/p2"}})(),
    ]
    out = _parse_input_file_name_rows(rows)
    assert out == [
        "abfss://c@n.dfs.core.windows.net/x/p1",
        "abfss://c@n.dfs.core.windows.net/x/p2",
    ]
