import pytest

from utils.discovery import ObjectRecord
from utils.migration import (
    plan_managed_delta_migration,
    plan_managed_non_delta_migration,
    plan_external_table_migration,
    plan_external_volume_migration,
    derive_pre_migration_fqn,
    derive_staging_fqn,
    build_create_managed_volume_sql,
    build_drop_volume_sql,
    build_rename_volume_sql,
    compare_volume_listings,
    build_volume_copy_pairs,
    plan_resumable_volume_copy,
    compare_volume_content_hashes,
    select_checksum_sample,
)


def test_build_create_managed_volume_sql_has_no_location():
    # Managed volume = no LOCATION clause (UC places it at the schema location).
    assert build_create_managed_volume_sql("c", "s", "v") == "CREATE VOLUME `c`.`s`.`v`"


def test_build_create_managed_volume_sql_if_not_exists():
    # Resumable re-runs reuse a staging volume left by a prior timed-out run.
    assert build_create_managed_volume_sql("c", "s", "v", if_not_exists=True) == (
        "CREATE VOLUME IF NOT EXISTS `c`.`s`.`v`"
    )


def test_build_drop_volume_sql():
    assert build_drop_volume_sql("c", "s", "v") == "DROP VOLUME `c`.`s`.`v`"


def test_build_rename_volume_sql_qualifies_target():
    # Target must be fully qualified (bare → CANNOT_RENAME_ACROSS_SCHEMA).
    assert build_rename_volume_sql("c", "s", "v__migrate_staging", "v") == (
        "ALTER VOLUME `c`.`s`.`v__migrate_staging` RENAME TO `c`.`s`.`v`"
    )


def test_compare_volume_listings_match():
    old = [("a/1.bin", 100), ("b/2.bin", 200)]
    new = [("b/2.bin", 200), ("a/1.bin", 100)]  # order-independent
    match, ev = compare_volume_listings(old, new)
    assert match is True
    assert ev["old_total_bytes"] == 300 and ev["new_total_bytes"] == 300


def test_compare_volume_listings_missing_file_blocks():
    old = [("a/1.bin", 100), ("b/2.bin", 200)]
    new = [("a/1.bin", 100)]
    match, ev = compare_volume_listings(old, new)
    assert match is False
    assert ev["missing"] == ["b/2.bin"]


def test_compare_volume_listings_size_mismatch_blocks():
    old = [("a/1.bin", 100)]
    new = [("a/1.bin", 101)]
    match, ev = compare_volume_listings(old, new)
    assert match is False
    assert ev["size_mismatch"] == ["a/1.bin"]


def test_compare_volume_listings_extra_file_blocks():
    old = [("a/1.bin", 100)]
    new = [("a/1.bin", 100), ("c/3.bin", 5)]
    match, ev = compare_volume_listings(old, new)
    assert match is False
    assert ev["extra"] == ["c/3.bin"]


def test_build_volume_copy_pairs_joins_relpaths():
    pairs = build_volume_copy_pairs(
        "/Volumes/c/s/v", "/Volumes/c/s/v__migrate_staging",
        [("a/1.bin", 100), ("b/2.bin", 200)],
    )
    assert pairs == [
        ("/Volumes/c/s/v/a/1.bin", "/Volumes/c/s/v__migrate_staging/a/1.bin", "a/1.bin", 100),
        ("/Volumes/c/s/v/b/2.bin", "/Volumes/c/s/v__migrate_staging/b/2.bin", "b/2.bin", 200),
    ]


def test_build_volume_copy_pairs_normalizes_trailing_slashes():
    # Roots with trailing slashes must not produce doubled separators.
    pairs = build_volume_copy_pairs("/Volumes/c/s/v/", "/Volumes/c/s/stg/", [("f.bin", 5)])
    assert pairs == [("/Volumes/c/s/v/f.bin", "/Volumes/c/s/stg/f.bin", "f.bin", 5)]


def test_plan_resumable_volume_copy_empty_target_copies_all():
    src = [("a/1.bin", 100), ("b/2.bin", 200)]
    to_copy, done = plan_resumable_volume_copy(src, [])
    assert to_copy == src
    assert done == []


def test_plan_resumable_volume_copy_full_target_copies_none():
    src = [("a/1.bin", 100), ("b/2.bin", 200)]
    to_copy, done = plan_resumable_volume_copy(src, list(reversed(src)))
    assert to_copy == []
    assert done == src


def test_plan_resumable_volume_copy_partial_target_resumes_remainder():
    src = [("a/1.bin", 100), ("b/2.bin", 200), ("c/3.bin", 300)]
    dst = [("a/1.bin", 100)]  # only first file landed before a timeout
    to_copy, done = plan_resumable_volume_copy(src, dst)
    assert to_copy == [("b/2.bin", 200), ("c/3.bin", 300)]
    assert done == [("a/1.bin", 100)]


def test_plan_resumable_volume_copy_size_mismatch_is_recopied():
    # A half-written file (wrong size) must be re-copied, not treated as done.
    src = [("a/1.bin", 100)]
    dst = [("a/1.bin", 40)]
    to_copy, done = plan_resumable_volume_copy(src, dst)
    assert to_copy == [("a/1.bin", 100)]
    assert done == []


def test_compare_volume_content_hashes_match():
    src = [("a/1.bin", "deadbeef"), ("b/2.bin", "cafef00d")]
    tgt = [("b/2.bin", "cafef00d"), ("a/1.bin", "deadbeef")]  # order-independent
    match, ev = compare_volume_content_hashes(src, tgt)
    assert match is True
    assert ev["source_file_count"] == 2 and ev["target_file_count"] == 2


def test_compare_volume_content_hashes_hash_mismatch_blocks():
    # Same path + size would pass count+size, but content differs → must block.
    src = [("a/1.bin", "deadbeef")]
    tgt = [("a/1.bin", "00000000")]
    match, ev = compare_volume_content_hashes(src, tgt)
    assert match is False
    assert ev["hash_mismatch"] == ["a/1.bin"]


def test_compare_volume_content_hashes_missing_and_extra_block():
    src = [("a/1.bin", "h1"), ("b/2.bin", "h2")]
    tgt = [("a/1.bin", "h1"), ("c/3.bin", "h3")]
    match, ev = compare_volume_content_hashes(src, tgt)
    assert match is False
    assert ev["missing"] == ["b/2.bin"]
    assert ev["extra"] == ["c/3.bin"]


def test_select_checksum_sample_full_fraction_returns_all():
    listing = [("a", 1), ("b", 2), ("c", 3)]
    assert select_checksum_sample(listing, 1.0) == listing
    assert select_checksum_sample(listing, 2.0) == listing  # clamped


def test_select_checksum_sample_zero_fraction_returns_none():
    assert select_checksum_sample([("a", 1), ("b", 2)], 0.0) == []


def test_select_checksum_sample_is_deterministic_by_relpath():
    # Source and target must pick the SAME files, regardless of listing order,
    # so their sampled hash sets are comparable.
    src = [("a/1", 1), ("b/2", 2), ("c/3", 3), ("d/4", 4), ("e/5", 5)]
    tgt = list(reversed(src))
    s = {rel for rel, _ in select_checksum_sample(src, 0.5)}
    t = {rel for rel, _ in select_checksum_sample(tgt, 0.5)}
    assert s == t


def test_select_checksum_sample_subset_grows_monotonically():
    # A larger fraction must be a superset of a smaller one (stable thresholding).
    listing = [(f"f/{i}", i) for i in range(200)]
    small = {rel for rel, _ in select_checksum_sample(listing, 0.2)}
    big = {rel for rel, _ in select_checksum_sample(listing, 0.6)}
    assert small.issubset(big)
    assert len(small) < len(big)


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
