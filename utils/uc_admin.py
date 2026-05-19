"""UC admin operations not exposed via SQL on native UC catalogs.

Some UC management operations are gated at the SQL surface for native
managed catalogs even though the underlying REST API supports them. The
most consequential one for storage migration is repointing a schema's
`storage_root`:

- SQL `ALTER SCHEMA <fqn> SET MANAGED LOCATION '...'` is rejected on
  native UC catalogs with `UC_COMMAND_NOT_SUPPORTED.NON_HMS_FEDERATED_ENTITY`.
- REST `PATCH /api/2.1/unity-catalog/schemas/<full_name>` with
  `{"storage_root": "..."}` works.

Use these helpers from any Databricks notebook with the bundled SDK.
"""
from __future__ import annotations

from typing import Optional, Protocol


class _ApiClient(Protocol):
    """Minimal interface for w.api_client (databricks-sdk WorkspaceClient)."""

    def do(self, method: str, path: str, body: Optional[dict] = None) -> dict: ...  # pragma: no cover


def set_schema_storage_root(
    *, api_client: _ApiClient, catalog: str, schema: str, storage_root: str,
) -> dict:
    """Set a schema's `storage_root` via the UC REST API.

    Works on both native UC and HMS-federated catalogs. Returns the full
    schema dict UC sent back (includes the updated `storage_root` plus the
    auto-allocated `storage_location` under it).

    NOTE: this does not move existing tables. Per Databricks docs, only
    NEW managed tables/volumes use the updated location. Existing objects
    retain their current physical paths.
    """
    return api_client.do(
        method="PATCH",
        path=f"/api/2.1/unity-catalog/schemas/{catalog}.{schema}",
        body={"storage_root": storage_root},
    )


def get_schema_storage_root(
    *, api_client: _ApiClient, catalog: str, schema: str,
) -> Optional[str]:
    """Return the schema's current `storage_root`, or None if not set."""
    resp = api_client.do(
        method="GET",
        path=f"/api/2.1/unity-catalog/schemas/{catalog}.{schema}",
    )
    return resp.get("storage_root")


def set_catalog_storage_root(
    *, api_client: _ApiClient, catalog: str, storage_root: str,
) -> dict:
    """Set a catalog's `storage_root` via the UC REST API.

    SQL `ALTER CATALOG <name> SET MANAGED LOCATION '...'` is rejected on
    this and likely other native UC workspaces with "command is not
    enabled here". The REST endpoint accepts the same change.

    Same semantics as schema repointing: existing managed objects under
    the catalog retain their physical paths; only new tables/schemas
    placed at the catalog default use the new path.
    """
    return api_client.do(
        method="PATCH",
        path=f"/api/2.1/unity-catalog/catalogs/{catalog}",
        body={"storage_root": storage_root},
    )
