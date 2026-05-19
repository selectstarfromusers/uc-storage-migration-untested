import pytest

from utils.discovery import ObjectRecord
from utils.migration import (
    plan_managed_delta_migration,
    plan_managed_non_delta_migration,
    plan_external_table_migration,
    plan_external_volume_migration,
    derive_pre_migration_fqn,
    derive_staging_fqn,
)


def make_rec(*, table_type="MANAGED", data_source_format="DELTA"):
    return ObjectRecord(
        catalog="c", schema="s", name="t",
        object_type="TABLE", table_type=table_type,
        data_source_format=data_source_format,
        storage_path="abfss://x@old.dfs.core.windows.net/t",
        parent_managed_location="abfss://x@new.dfs.core.windows.net/",
        owner="alice", created_at=None, last_altered=None,
    )


def test_derive_pre_migration_fqn():
    assert derive_pre_migration_fqn("c", "s", "t") == ("c", "s", "t__pre_migration")


def test_derive_staging_fqn():
    assert derive_staging_fqn("c", "s", "t") == ("c", "s", "t__migrate_staging")


class TestPlanManagedDeltaMigration:
    def test_emits_clone_then_rename_swap_in_order(self):
        plan = plan_managed_delta_migration(rec=make_rec())
        assert plan.steps == [
            ("clone",
             "CREATE TABLE `c`.`s`.`t__migrate_staging` DEEP CLONE `c`.`s`.`t`"),
            ("rename_orig",
             "ALTER TABLE `c`.`s`.`t` RENAME TO `c`.`s`.`t__pre_migration`"),
            ("rename_staging",
             "ALTER TABLE `c`.`s`.`t__migrate_staging` RENAME TO `c`.`s`.`t`"),
        ]


class TestPlanManagedNonDelta:
    def test_uses_ctas_not_deep_clone(self):
        plan = plan_managed_non_delta_migration(rec=make_rec(data_source_format="ICEBERG"))
        assert any("CREATE TABLE" in sql and "AS SELECT * FROM" in sql for _, sql in plan.steps)
        assert not any("DEEP CLONE" in sql for _, sql in plan.steps)


class TestPlanExternalTable:
    def test_drops_and_creates_at_new_location(self):
        rec = make_rec(table_type="EXTERNAL")
        plan = plan_external_table_migration(rec=rec, new_storage_account="new")
        actions = [k for k, _ in plan.steps]
        assert actions == ["drop", "create"]
        assert "DROP TABLE" in plan.steps[0][1]
        assert "CREATE EXTERNAL TABLE" in plan.steps[1][1]
        assert "abfss://x@new.dfs.core.windows.net/t" in plan.steps[1][1]


class TestPlanExternalVolume:
    def test_drops_and_creates_volume(self):
        rec = ObjectRecord(
            catalog="c", schema="s", name="v",
            object_type="VOLUME", table_type="EXTERNAL",
            data_source_format=None,
            storage_path="abfss://x@old.dfs.core.windows.net/v",
            parent_managed_location=None, owner="u",
            created_at=None, last_altered=None,
        )
        plan = plan_external_volume_migration(rec=rec, new_storage_account="new")
        assert any("DROP VOLUME" in sql for _, sql in plan.steps)
        assert any("CREATE EXTERNAL VOLUME" in sql for _, sql in plan.steps)
        assert any("abfss://x@new.dfs.core.windows.net/v" in sql for _, sql in plan.steps)


# --- S3 AWS support ---

def make_s3_rec(*, table_type="EXTERNAL", data_source_format="DELTA"):
    return ObjectRecord(
        catalog="c", schema="s", name="t",
        object_type="TABLE", table_type=table_type,
        data_source_format=data_source_format,
        storage_path="s3://old-bucket/path/to/t",
        parent_managed_location="s3://new-bucket/path/to/",
        owner="alice", created_at=None, last_altered=None,
    )


class TestRewriteAccountInPath:
    def test_rewrites_abfss(self):
        from utils.migration import rewrite_account_in_path
        out = rewrite_account_in_path("abfss://c@old.dfs.core.windows.net/p", "new")
        assert out == "abfss://c@new.dfs.core.windows.net/p"

    def test_rewrites_s3(self):
        from utils.migration import rewrite_account_in_path
        out = rewrite_account_in_path("s3://old-bucket/path/to/obj", "new-bucket")
        assert out == "s3://new-bucket/path/to/obj"

    def test_rewrites_s3a_to_canonical_s3(self):
        from utils.migration import rewrite_account_in_path
        out = rewrite_account_in_path("s3a://old-bucket/p", "new-bucket")
        assert out == "s3://new-bucket/p"

    def test_raises_on_unrecognized(self):
        from utils.migration import rewrite_account_in_path
        try:
            rewrite_account_in_path("file:///tmp/x", "new")
            assert False, "expected ValueError"
        except ValueError as e:
            assert "Not a recognized storage URL" in str(e)


class TestPlanExternalTableOnS3:
    def test_drops_and_creates_at_new_s3_bucket(self):
        rec = make_s3_rec()
        plan = plan_external_table_migration(rec=rec, new_storage_account="new-bucket")
        assert plan.steps[0] == ("drop", "DROP TABLE `c`.`s`.`t`")
        create_sql = plan.steps[1][1]
        assert "CREATE EXTERNAL TABLE `c`.`s`.`t` USING DELTA" in create_sql
        assert "'s3://new-bucket/path/to/t'" in create_sql


class TestPlanExternalVolumeOnS3:
    def test_drops_and_creates_volume_at_new_bucket(self):
        rec = ObjectRecord(
            catalog="c", schema="s", name="v",
            object_type="VOLUME", table_type="EXTERNAL",
            data_source_format=None,
            storage_path="s3://old-bucket/vol/v",
            parent_managed_location=None, owner="u",
            created_at=None, last_altered=None,
        )
        plan = plan_external_volume_migration(rec=rec, new_storage_account="new-bucket")
        assert any("DROP VOLUME" in sql for _, sql in plan.steps)
        assert any("'s3://new-bucket/vol/v'" in sql for _, sql in plan.steps)
