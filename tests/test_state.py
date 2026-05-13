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
