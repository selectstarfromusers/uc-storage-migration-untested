from unittest.mock import MagicMock

import pytest

from utils.uc_client import (
    UcClient,
    CatalogRecord,
    SchemaRecord,
    ExternalLocationRecord,
    MetastoreInfo,
    StorageCredentialRecord,
)


def make_sdk_catalog(name, catalog_type, storage_root, owner):
    m = MagicMock()
    m.name = name
    m.catalog_type = catalog_type
    m.storage_root = storage_root
    m.owner = owner
    m.comment = None
    m.isolation_mode = None
    return m


def make_sdk_schema(name, catalog, storage_root, owner):
    m = MagicMock()
    m.name = name
    m.catalog_name = catalog
    m.storage_root = storage_root
    m.owner = owner
    return m


class TestListCatalogs:
    def test_returns_catalog_records(self):
        sdk = MagicMock()
        sdk.catalogs.list.return_value = [
            make_sdk_catalog("c1", "MANAGED_CATALOG", "abfss://c@oldacct.dfs.core.windows.net/c1", "u1"),
            make_sdk_catalog("c2", "FOREIGN_CATALOG", None, "u2"),
        ]
        client = UcClient(sdk=sdk, rest=MagicMock())

        result = client.list_catalogs()

        assert len(result) == 2
        assert result[0] == CatalogRecord(
            name="c1",
            catalog_type="MANAGED_CATALOG",
            storage_root="abfss://c@oldacct.dfs.core.windows.net/c1",
            owner="u1",
            comment=None,
            isolation_mode=None,
        )
        assert result[1].catalog_type == "FOREIGN_CATALOG"

    def test_filters_by_allowlist(self):
        sdk = MagicMock()
        sdk.catalogs.list.return_value = [
            make_sdk_catalog("c1", "MANAGED_CATALOG", None, "u"),
            make_sdk_catalog("c2", "MANAGED_CATALOG", None, "u"),
            make_sdk_catalog("c3", "MANAGED_CATALOG", None, "u"),
        ]
        client = UcClient(sdk=sdk, rest=MagicMock())

        result = client.list_catalogs(allowlist=["c1", "c3"])

        assert [c.name for c in result] == ["c1", "c3"]


class TestListSchemas:
    def test_returns_schema_records(self):
        sdk = MagicMock()
        sdk.schemas.list.return_value = [
            make_sdk_schema("s1", "c1", "abfss://c@new.dfs.core.windows.net/s1", "u"),
        ]
        client = UcClient(sdk=sdk, rest=MagicMock())

        result = client.list_schemas("c1")

        assert result[0] == SchemaRecord(
            name="s1", catalog_name="c1",
            storage_root="abfss://c@new.dfs.core.windows.net/s1", owner="u",
        )


class TestGetMetastore:
    def test_parses_metastore_response(self):
        rest = MagicMock()
        rest.get.return_value = {
            "metastore_id": "abc-123",
            "name": "test-ms",
            "storage_root": "abfss://root@oldacct.dfs.core.windows.net/",
            "region": "eastus",
        }
        client = UcClient(sdk=MagicMock(), rest=rest)

        result = client.get_metastore()

        assert result == MetastoreInfo(
            metastore_id="abc-123",
            name="test-ms",
            storage_root="abfss://root@oldacct.dfs.core.windows.net/",
            region="eastus",
        )
        rest.get.assert_called_once_with("/api/2.1/unity-catalog/metastores/current")


class TestListExternalLocations:
    def test_parses_external_locations(self):
        rest = MagicMock()
        rest.get.return_value = {
            "external_locations": [
                {
                    "name": "old_root",
                    "url": "abfss://c@oldacct.dfs.core.windows.net/",
                    "credential_name": "old_cred",
                    "read_only": False,
                },
                {
                    "name": "new_root",
                    "url": "abfss://c@newacct.dfs.core.windows.net/",
                    "credential_name": "new_cred",
                    "read_only": False,
                },
            ]
        }
        client = UcClient(sdk=MagicMock(), rest=rest)

        result = client.list_external_locations()

        assert len(result) == 2
        assert result[0] == ExternalLocationRecord(
            name="old_root",
            url="abfss://c@oldacct.dfs.core.windows.net/",
            credential_name="old_cred",
            read_only=False,
        )


class TestListStorageCredentials:
    def test_parses_credentials(self):
        rest = MagicMock()
        rest.get.return_value = {
            "storage_credentials": [
                {
                    "name": "old_cred",
                    "owner": "u1",
                    "read_only": False,
                    "used_for_managed_storage": True,
                    "azure_managed_identity": {"access_connector_id": "x"},
                },
                {
                    "name": "new_cred",
                    "owner": "u2",
                    "read_only": True,
                    "azure_service_principal": {"client_id": "y"},
                },
            ]
        }
        client = UcClient(sdk=MagicMock(), rest=rest)

        result = client.list_storage_credentials()

        assert len(result) == 2
        assert result[0] == StorageCredentialRecord(
            name="old_cred",
            credential_type="azure_managed_identity",
            owner="u1",
            read_only=False,
            used_for_managed_storage=True,
        )
        assert result[1].credential_type == "azure_service_principal"
        assert result[1].read_only is True
        rest.get.assert_called_with("/api/2.1/unity-catalog/storage-credentials")
