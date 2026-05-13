"""Resolve a UC object's storage path with DESCRIBE EXTENDED as fallback.

Some metastore versions or asset types do not populate `storage_path` in
`system.information_schema.tables`. When that happens, `DESCRIBE EXTENDED <fqn>`
exposes the Location field directly. This module wraps that fallback in a
single helper so discovery code can be uniform: "resolve the storage path
for this object" works regardless of which path is populated.
"""
from __future__ import annotations

from typing import Optional, Protocol

from utils.sql import parse_describe_extended_location, quote_fqn


class _SqlExec(Protocol):
    def sql(self, query: str):  # pragma: no cover - Spark interface
        ...


def resolve_storage_path(
    *,
    spark: _SqlExec,
    catalog: str,
    schema: str,
    name: str,
    info_schema_path: Optional[str],
    object_type: str = "TABLE",
) -> Optional[str]:
    """Return the object's storage path.

    Prefers the info_schema value when present. Falls back to DESCRIBE EXTENDED
    when the info_schema field is null. Returns None if the fallback also fails
    (e.g., the caller lacks BROWSE on the object, or it is a view with no
    underlying storage).
    """
    if info_schema_path:
        return info_schema_path

    fqn = quote_fqn(catalog, schema, name)
    # Databricks grammar: DESCRIBE [TABLE|VOLUME] [EXTENDED] name. EXTENDED only
    # applies to tables; volumes use DESCRIBE VOLUME (no EXTENDED).
    if object_type == "VOLUME":
        describe_sql = f"DESCRIBE VOLUME {fqn}"
    else:
        describe_sql = f"DESCRIBE TABLE EXTENDED {fqn}"
    try:
        rows = spark.sql(describe_sql).collect()
    except Exception:
        return None

    # Spark renders DESCRIBE EXTENDED as a 2-column DataFrame (col_name, data_type, ...).
    # Stringify rows so the regex-based parser works uniformly.
    rendered = "\n".join(
        "\t".join(str(c) if c is not None else "" for c in row) for row in rows
    )
    return parse_describe_extended_location(rendered)
