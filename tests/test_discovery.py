import pytest

from utils.discovery import (
    ObjectRecord,
    classify_object,
    Classification,
)


def make_record(
    *,
    object_type="TABLE",
    table_type="MANAGED",
    storage_path=None,
    parent_managed_location=None,
):
    return ObjectRecord(
        catalog="c",
        schema="s",
        name="o",
        object_type=object_type,
        table_type=table_type,
        data_source_format="DELTA",
        storage_path=storage_path,
        parent_managed_location=parent_managed_location,
        owner="u",
        created_at=None,
        last_altered=None,
    )


class TestClassifyObject:
    def test_managed_on_old_parent_old_is_consistent_old(self):
        rec = make_record(
            table_type="MANAGED",
            storage_path="abfss://c@oldacct.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@oldacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "consistent_old"

    def test_managed_on_new_parent_new_is_consistent_new(self):
        rec = make_record(
            table_type="MANAGED",
            storage_path="abfss://c@newacct.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "consistent_new"

    def test_managed_on_old_parent_new_is_drift(self):
        rec = make_record(
            table_type="MANAGED",
            storage_path="abfss://c@oldacct.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "drift_managed_on_old"

    def test_external_on_old_is_external_on_old(self):
        rec = make_record(
            table_type="EXTERNAL",
            storage_path="abfss://c@oldacct.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "external_on_old"

    def test_external_on_new_is_external_on_new(self):
        rec = make_record(
            table_type="EXTERNAL",
            storage_path="abfss://c@newacct.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "external_on_new"

    def test_unknown_account_path(self):
        rec = make_record(
            table_type="MANAGED",
            storage_path="abfss://c@thirdparty.dfs.core.windows.net/x",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "unknown_account"

    def test_null_storage_path_is_path_missing(self):
        rec = make_record(table_type="MANAGED", storage_path=None)
        assert classify_object(rec, old="oldacct", new="newacct") == "path_missing"

    def test_view_is_path_missing(self):
        rec = make_record(object_type="TABLE", table_type="VIEW", storage_path=None)
        assert classify_object(rec, old="oldacct", new="newacct") == "path_missing"

    def test_volume_on_old_is_classified_like_table(self):
        rec = make_record(
            object_type="VOLUME",
            table_type="EXTERNAL",
            storage_path="abfss://c@oldacct.dfs.core.windows.net/v",
            parent_managed_location="abfss://c@newacct.dfs.core.windows.net/",
        )
        assert classify_object(rec, old="oldacct", new="newacct") == "external_on_old"
