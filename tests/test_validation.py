from unittest.mock import MagicMock

import pytest

from utils.validation import (
    EvidenceLayer,
    ValidationResult,
    validate_object_on_new,
    build_content_checksum_sql,
    compare_content_checksum,
    _parse_input_file_name_rows,
    _hosts_in_paths,
)


def _row(d):
    return type("R", (), {"asDict": lambda self, _d=d: dict(_d)})()


def _result_mock(rows):
    m = MagicMock()
    m.collect.return_value = rows
    return m


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
    # Layer 2 (_delta_log ls) only runs for EXTERNAL Delta; for a managed table
    # it's N/A (Layer 1 already proves location).
    assert result.delta_log_at_new_ok is None
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


def test_build_content_checksum_sql():
    sql = build_content_checksum_sql("`c`.`s`.`t`")
    assert "count(*) AS n" in sql
    assert "bit_xor(xxhash64(*))" in sql
    assert "sum(cast(xxhash64(*) AS decimal(38,0)))" in sql
    assert sql.strip().endswith("FROM `c`.`s`.`t`")


def test_compare_content_checksum_match():
    spark = MagicMock()
    spark.sql.side_effect = [
        _result_mock([_row({"n": 5, "xor64": 111, "sum64": 222})]),  # source
        _result_mock([_row({"n": 5, "xor64": 111, "sum64": 222})]),  # target
    ]
    match, ev = compare_content_checksum(spark, source_fqn="`c`.`s`.`t__pre_migration`", target_fqn="`c`.`s`.`t`")
    assert match is True
    assert ev["match"] is True


def test_compare_content_checksum_mismatch_on_rowcount():
    spark = MagicMock()
    spark.sql.side_effect = [
        _result_mock([_row({"n": 5, "xor64": 111, "sum64": 222})]),
        _result_mock([_row({"n": 6, "xor64": 111, "sum64": 222})]),  # n differs
    ]
    match, ev = compare_content_checksum(spark, source_fqn="src", target_fqn="tgt")
    assert match is False


def test_compare_content_checksum_mismatch_on_multiplicity():
    # Same XOR (duplicate cancels) but different SUM and count → must be caught.
    spark = MagicMock()
    spark.sql.side_effect = [
        _result_mock([_row({"n": 2, "xor64": 0, "sum64": 200})]),
        _result_mock([_row({"n": 4, "xor64": 0, "sum64": 400})]),
    ]
    match, _ = compare_content_checksum(spark, source_fqn="src", target_fqn="tgt")
    assert match is False


def test_validate_content_checksum_mismatch_blocks_overall_pass():
    spark = MagicMock()
    fs = MagicMock()
    fs.ls.return_value = [MagicMock()]
    spark.sql.side_effect = [
        # Layer 1 DESCRIBE → location on new
        _result_mock([_row({"col_name": "Location", "data_type": "abfss://c@newacct.dfs.core.windows.net/x"})]),
        # Layer 3 _metadata.file_path → on new
        _result_mock([_row({"path": "abfss://c@newacct.dfs.core.windows.net/x/p0.parquet"})]),
        # Layer 5 checksum: source then target — mismatch
        _result_mock([_row({"n": 100, "xor64": 7, "sum64": 700})]),
        _result_mock([_row({"n": 100, "xor64": 7, "sum64": 701})]),  # sum64 differs
    ]
    result = validate_object_on_new(
        spark=spark, fs=fs, catalog="c", schema="s", name="t",
        expected_new_account="newacct",
        parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        is_delta=True, is_external=False,
        verify_content_checksum=True, compare_fqn="`c`.`s`.`t__pre_migration`",
    )
    assert result.content_checksum_ok is False
    assert result.overall_pass is False  # mismatch blocks


def test_validate_content_checksum_external_is_na():
    spark = MagicMock()
    fs = MagicMock()
    fs.ls.return_value = [MagicMock()]
    spark.sql.side_effect = [
        _result_mock([_row({"col_name": "Location", "data_type": "abfss://c@newacct.dfs.core.windows.net/x"})]),
        _result_mock([_row({"path": "abfss://c@newacct.dfs.core.windows.net/x/p0.parquet"})]),
    ]
    result = validate_object_on_new(
        spark=spark, fs=fs, catalog="c", schema="s", name="t",
        expected_new_account="newacct", parent_managed_location=None,
        is_delta=True, is_external=True,
        verify_content_checksum=True, compare_fqn=None,
    )
    assert result.content_checksum_ok is None  # N/A for external


def test_validate_volume_uses_information_schema_location():
    """Volumes: location from information_schema.volumes; row-based layers + the
    content checksum are N/A; overall_pass rests on location + parent."""
    spark = MagicMock()
    fs = MagicMock()
    spark.sql.side_effect = [
        _result_mock([_row({"storage_location": "abfss://x@newacct.dfs.core.windows.net/v"})]),
    ]
    result = validate_object_on_new(
        spark=spark, fs=fs, catalog="c", schema="s", name="v",
        expected_new_account="newacct",
        parent_managed_location="abfss://x@newacct.dfs.core.windows.net/",
        is_delta=False, is_external=False, object_type="VOLUME",
        verify_content_checksum=True, compare_fqn="`c`.`s`.`v__pre_migration`",
    )
    assert result.metadata_location_ok is True
    assert result.input_file_name_ok is None       # N/A for volume
    assert result.content_checksum_ok is None       # N/A for volume
    assert result.parent_managed_location_match is True
    assert result.overall_pass is True


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
