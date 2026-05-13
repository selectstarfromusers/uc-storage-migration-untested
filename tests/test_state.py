import pytest

from utils.discovery import ObjectRecord
from utils.state import InventoryWriter, INVENTORY_SCHEMA


def test_inventory_schema_contains_expected_columns():
    expected = {
        "catalog",
        "schema",
        "name",
        "object_type",
        "table_type",
        "data_source_format",
        "storage_path",
        "parent_managed_location",
        "owner",
        "classification",
        "captured_at",
    }
    actual = set(INVENTORY_SCHEMA.fieldNames())
    assert expected.issubset(actual)


def test_write_inventory_creates_dataframe_with_classification(spark, tmp_path):
    writer = InventoryWriter(spark=spark)
    records = [
        (
            ObjectRecord(
                catalog="c", schema="s", name="t1",
                object_type="TABLE", table_type="MANAGED",
                data_source_format="DELTA",
                storage_path="abfss://c@oldacct.dfs.core.windows.net/x",
                parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
                owner="u", created_at=None, last_altered=None,
            ),
            "drift_managed_on_old",
        ),
    ]

    df = writer.records_to_dataframe(records)

    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["classification"] == "drift_managed_on_old"
    assert rows[0]["catalog"] == "c"


def test_migration_log_schema_has_required_columns():
    from utils.state import MIGRATION_LOG_SCHEMA
    names = set(MIGRATION_LOG_SCHEMA.fieldNames())
    required = {"catalog", "schema", "name", "object_type", "status",
                "claimed_by", "claimed_at", "staging_fqn", "pre_migration_fqn",
                "error_trace", "updated_at"}
    assert required.issubset(names)


def test_snapshot_schema_has_required_columns():
    from utils.state import OBJECT_METADATA_SNAPSHOT_SCHEMA
    names = set(OBJECT_METADATA_SNAPSHOT_SCHEMA.fieldNames())
    assert {"catalog", "schema", "name", "snapshot_json", "captured_at"}.issubset(names)


def test_validation_results_schema_has_required_columns():
    from utils.state import VALIDATION_RESULTS_SCHEMA
    names = set(VALIDATION_RESULTS_SCHEMA.fieldNames())
    required = {
        "metadata_location_ok", "delta_log_at_new_ok", "input_file_name_ok",
        "parent_managed_location_match", "grants_ok", "owner_ok",
        "overall_pass", "evidence_json", "validated_at",
    }
    assert required.issubset(names)


def test_migration_log_to_sql_literal_handles_types():
    from utils.state import MigrationLog
    from datetime import datetime
    assert MigrationLog._to_sql_literal(None) == "NULL"
    assert MigrationLog._to_sql_literal(True) == "TRUE"
    assert MigrationLog._to_sql_literal(42) == "42"
    assert MigrationLog._to_sql_literal("it's") == "'it''s'"
    ts = datetime(2026, 5, 13, 10, 0, 0)
    assert MigrationLog._to_sql_literal(ts) == "TIMESTAMP '2026-05-13T10:00:00'"
