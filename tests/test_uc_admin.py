from unittest.mock import MagicMock

from utils.uc_admin import (
    set_schema_storage_root,
    get_schema_storage_root,
    set_catalog_storage_root,
)


class TestSetSchemaStorageRoot:
    def test_patches_schema_with_storage_root(self):
        client = MagicMock()
        client.do.return_value = {"storage_root": "s3://new/root", "full_name": "c.s"}

        result = set_schema_storage_root(
            api_client=client, catalog="cat", schema="sch", storage_root="s3://new/root",
        )

        client.do.assert_called_once_with(
            method="PATCH",
            path="/api/2.1/unity-catalog/schemas/cat.sch",
            body={"storage_root": "s3://new/root"},
        )
        assert result["storage_root"] == "s3://new/root"

    def test_abfss_url(self):
        """Azure URLs work the same — just a different URL string."""
        client = MagicMock()
        client.do.return_value = {"storage_root": "abfss://c@new.dfs.core.windows.net/p"}

        result = set_schema_storage_root(
            api_client=client, catalog="cat", schema="sch",
            storage_root="abfss://c@new.dfs.core.windows.net/p",
        )

        sent_body = client.do.call_args.kwargs["body"]
        assert sent_body["storage_root"].startswith("abfss://")


class TestGetSchemaStorageRoot:
    def test_returns_storage_root_from_get(self):
        client = MagicMock()
        client.do.return_value = {"storage_root": "s3://existing/root", "full_name": "c.s"}

        result = get_schema_storage_root(api_client=client, catalog="cat", schema="sch")

        client.do.assert_called_once_with(
            method="GET",
            path="/api/2.1/unity-catalog/schemas/cat.sch",
        )
        assert result == "s3://existing/root"

    def test_returns_none_when_not_set(self):
        """Schemas inheriting from catalog default have no explicit storage_root."""
        client = MagicMock()
        client.do.return_value = {"full_name": "c.s"}  # no storage_root key

        result = get_schema_storage_root(api_client=client, catalog="cat", schema="sch")

        assert result is None


class TestSetCatalogStorageRoot:
    def test_patches_catalog_with_storage_root(self):
        client = MagicMock()
        client.do.return_value = {"storage_root": "s3://new/root", "name": "cat"}

        result = set_catalog_storage_root(
            api_client=client, catalog="cat", storage_root="s3://new/root",
        )

        client.do.assert_called_once_with(
            method="PATCH",
            path="/api/2.1/unity-catalog/catalogs/cat",
            body={"storage_root": "s3://new/root"},
        )
        assert result["storage_root"] == "s3://new/root"
