"""Tests for utils/config.py — smart defaults, validation, helpers.

These run under pytest. They mutate the module-level globals; the
fixture restores them to safe defaults between tests.
"""
import pytest

from utils import config as cfg


@pytest.fixture(autouse=True)
def _reset_config():
    """Snapshot + restore module-level config between tests."""
    snap = {
        "OLD_STORAGE_ACCOUNT": cfg.OLD_STORAGE_ACCOUNT,
        "NEW_STORAGE_ACCOUNT": cfg.NEW_STORAGE_ACCOUNT,
        "OPS_SCHEMA": cfg.OPS_SCHEMA,
        "CATALOG_ALLOWLIST": list(cfg.CATALOG_ALLOWLIST),
        "ALLOW_ALL_CATALOGS": cfg.ALLOW_ALL_CATALOGS,
        "REPOINT_CATALOG": cfg.REPOINT_CATALOG,
        "SCHEMAS_TO_REPOINT": cfg.SCHEMAS_TO_REPOINT,
        "NEW_STORAGE_PREFIX": cfg.NEW_STORAGE_PREFIX,
        "POST_VALIDATION_CLEANUP_OK": cfg.POST_VALIDATION_CLEANUP_OK,
        "ALLOW_MANAGED_VOLUMES_SKIP": cfg.ALLOW_MANAGED_VOLUMES_SKIP,
    }
    yield
    for k, v in snap.items():
        setattr(cfg, k, v)


class TestResolveConfigSmartDefaults:
    def test_ops_schema_derives_from_first_catalog(self):
        cfg.CATALOG_ALLOWLIST = ["my_catalog"]
        cfg.OPS_SCHEMA = None
        cfg.resolve_config(spark=None)
        assert cfg.OPS_SCHEMA == "my_catalog._migration_ops"

    def test_ops_schema_explicit_overrides(self):
        cfg.CATALOG_ALLOWLIST = ["my_catalog"]
        cfg.OPS_SCHEMA = "other_catalog._x"
        cfg.resolve_config(spark=None)
        assert cfg.OPS_SCHEMA == "other_catalog._x"

    def test_repoint_catalog_auto_when_single(self):
        cfg.CATALOG_ALLOWLIST = ["only_cat"]
        cfg.REPOINT_CATALOG = None
        cfg.resolve_config(spark=None)
        assert cfg.REPOINT_CATALOG == "only_cat"

    def test_repoint_catalog_stays_none_when_multiple(self):
        cfg.CATALOG_ALLOWLIST = ["cat1", "cat2"]
        cfg.REPOINT_CATALOG = None
        cfg.resolve_config(spark=None)
        assert cfg.REPOINT_CATALOG is None  # ambiguous — customer must pick

    def test_schemas_to_repoint_stays_none_without_spark(self):
        cfg.CATALOG_ALLOWLIST = ["c1"]
        cfg.SCHEMAS_TO_REPOINT = None
        cfg.resolve_config(spark=None)
        assert cfg.SCHEMAS_TO_REPOINT is None

    def test_resolve_is_idempotent(self):
        cfg.CATALOG_ALLOWLIST = ["a"]
        cfg.OPS_SCHEMA = None
        cfg.REPOINT_CATALOG = None
        cfg.resolve_config(spark=None)
        cfg.resolve_config(spark=None)
        assert cfg.OPS_SCHEMA == "a._migration_ops"
        assert cfg.REPOINT_CATALOG == "a"


class TestValidateConfigForDiscovery:
    def test_refuses_empty_allowlist(self):
        cfg.CATALOG_ALLOWLIST = []
        cfg.ALLOW_ALL_CATALOGS = False
        cfg.OPS_SCHEMA = "x._y"
        with pytest.raises(ValueError, match="CATALOG_ALLOWLIST is empty"):
            cfg.validate_config_for_discovery()

    def test_accepts_explicit_allow_all_catalogs(self):
        cfg.CATALOG_ALLOWLIST = []
        cfg.ALLOW_ALL_CATALOGS = True
        cfg.OPS_SCHEMA = "x._y"
        cfg.validate_config_for_discovery()  # no raise

    def test_refuses_unresolved_ops_schema(self):
        cfg.CATALOG_ALLOWLIST = ["c"]
        cfg.OPS_SCHEMA = None
        cfg.ALLOW_ALL_CATALOGS = False
        with pytest.raises(ValueError, match="OPS_SCHEMA could not be resolved"):
            cfg.validate_config_for_discovery()


class TestValidateConfigForRepoint:
    def test_refuses_empty_new_storage_prefix(self):
        cfg.NEW_STORAGE_PREFIX = ""
        cfg.REPOINT_CATALOG = "c"
        cfg.SCHEMAS_TO_REPOINT = ["s1"]
        with pytest.raises(ValueError, match="NEW_STORAGE_PREFIX is required"):
            cfg.validate_config_for_repoint()

    def test_refuses_empty_repoint_catalog(self):
        cfg.NEW_STORAGE_PREFIX = "s3://x/y"
        cfg.REPOINT_CATALOG = None
        cfg.SCHEMAS_TO_REPOINT = ["s1"]
        with pytest.raises(ValueError, match="REPOINT_CATALOG could not be resolved"):
            cfg.validate_config_for_repoint()

    def test_refuses_empty_schemas(self):
        cfg.NEW_STORAGE_PREFIX = "s3://x/y"
        cfg.REPOINT_CATALOG = "c"
        cfg.SCHEMAS_TO_REPOINT = []
        with pytest.raises(ValueError, match="SCHEMAS_TO_REPOINT is empty"):
            cfg.validate_config_for_repoint()

    def test_passes_when_all_set(self):
        cfg.NEW_STORAGE_PREFIX = "s3://x/y"
        cfg.REPOINT_CATALOG = "c"
        cfg.SCHEMAS_TO_REPOINT = ["s1", "s2"]
        cfg.validate_config_for_repoint()  # no raise
