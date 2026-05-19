"""Wrappers around databricks-sdk and UC REST endpoints, returning dataclasses."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass(frozen=True)
class CatalogRecord:
    name: str
    catalog_type: Optional[str]
    storage_root: Optional[str]
    owner: Optional[str]
    comment: Optional[str]
    isolation_mode: Optional[str]


@dataclass(frozen=True)
class SchemaRecord:
    name: str
    catalog_name: str
    storage_root: Optional[str]
    owner: Optional[str]


@dataclass(frozen=True)
class ExternalLocationRecord:
    name: str
    url: str
    credential_name: str
    read_only: bool
    # NOTE: UC's external-locations API does NOT return a region field. All
    # external locations in a metastore must share the metastore's region by
    # UC design, so cross-region cannot be derived from this list. Region
    # stays Optional[None] for backward compat — get the metastore region from
    # `w.metastores.summary().region` instead.
    region: Optional[str] = None
    isolation_mode: Optional[str] = None
    accessible_in_current_workspace: Optional[bool] = None


@dataclass(frozen=True)
class StorageCredentialRecord:
    name: str
    credential_type: str          # "AzureManagedIdentity" | "AzureServicePrincipal" | "AccessConnector" | ...
    owner: Optional[str]
    read_only: bool
    used_for_managed_storage: bool


@dataclass(frozen=True)
class MetastoreInfo:
    metastore_id: str
    name: str
    storage_root: Optional[str]
    region: Optional[str]


class _RestProto(Protocol):
    def get(self, path: str) -> dict: ...


class UcClient:
    """Thin wrapper around databricks-sdk + REST. Returns dataclasses, not SDK types."""

    def __init__(self, *, sdk, rest: _RestProto):
        self._sdk = sdk
        self._rest = rest

    def list_catalogs(self, *, allowlist: Optional[list[str]] = None) -> list[CatalogRecord]:
        records = [
            CatalogRecord(
                name=c.name,
                catalog_type=getattr(c, "catalog_type", None),
                storage_root=getattr(c, "storage_root", None),
                owner=getattr(c, "owner", None),
                comment=getattr(c, "comment", None),
                isolation_mode=getattr(c, "isolation_mode", None),
            )
            for c in self._sdk.catalogs.list()
        ]
        if allowlist:
            allow_set = set(allowlist)
            records = [r for r in records if r.name in allow_set]
        return records

    def list_schemas(self, catalog: str) -> list[SchemaRecord]:
        return [
            SchemaRecord(
                name=s.name,
                catalog_name=s.catalog_name,
                storage_root=getattr(s, "storage_root", None),
                owner=getattr(s, "owner", None),
            )
            for s in self._sdk.schemas.list(catalog_name=catalog)
        ]

    def get_metastore(self) -> MetastoreInfo:
        resp = self._rest.get("/api/2.1/unity-catalog/metastores/current")
        return MetastoreInfo(
            metastore_id=resp["metastore_id"],
            name=resp["name"],
            storage_root=resp.get("storage_root"),
            region=resp.get("region"),
        )

    def list_external_locations(self) -> list[ExternalLocationRecord]:
        resp = self._rest.get("/api/2.1/unity-catalog/external-locations")
        return [
            ExternalLocationRecord(
                name=el["name"],
                url=el["url"],
                credential_name=el["credential_name"],
                read_only=el.get("read_only", False),
                # Region is never present in the response; kept for back-compat.
                region=el.get("region"),
                isolation_mode=el.get("isolation_mode"),
                accessible_in_current_workspace=el.get("accessible_in_current_workspace"),
            )
            for el in resp.get("external_locations", [])
        ]

    def list_storage_credentials(self) -> list[StorageCredentialRecord]:
        resp = self._rest.get("/api/2.1/unity-catalog/storage-credentials")
        out = []
        for sc in resp.get("storage_credentials", []):
            cred_type = next(
                (k for k in ("azure_managed_identity", "azure_service_principal",
                             "azure_access_connector", "aws_iam_role", "gcp_service_account_key")
                 if k in sc),
                "unknown",
            )
            out.append(StorageCredentialRecord(
                name=sc["name"],
                credential_type=cred_type,
                owner=sc.get("owner"),
                read_only=sc.get("read_only", False),
                used_for_managed_storage=sc.get("used_for_managed_storage", False),
            ))
        return out
