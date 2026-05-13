"""Governance capture and replay: grants, owner, tags, filters, masks, comments."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from utils.sql import quote_ident, quote_fqn


@dataclass(frozen=True)
class GrantEntry:
    principal: str
    privilege: str
    object_type: str


@dataclass(frozen=True)
class TagEntry:
    name: str
    value: str


@dataclass(frozen=True)
class ColumnMaskEntry:
    column: str
    mask_function: str
    using_columns: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class GovernanceSnapshot:
    catalog: str
    schema: str
    name: str
    grants: list[GrantEntry]
    owner: Optional[str]
    tags: list[TagEntry]
    row_filter_name: Optional[str]
    row_filter_using_columns: list[str]
    column_masks: list[ColumnMaskEntry]
    table_comment: Optional[str]
    column_comments: dict[str, str]
    table_properties: dict[str, str]


# --- SQL builders for capture ---

def _object_keyword(object_type: str) -> str:
    return "VOLUME" if object_type == "VOLUME" else "TABLE"


def build_show_grants_sql(*, catalog: str, schema: str, name: str, object_type: str = "TABLE") -> str:
    return f"SHOW GRANTS ON {_object_keyword(object_type)} {quote_fqn(catalog, schema, name)}"


def build_show_tags_sql(*, catalog: str, schema: str, name: str) -> str:
    return (
        "SELECT tag_name, tag_value "
        "FROM system.information_schema.table_tags "
        f"WHERE catalog_name = '{catalog}' "
        f"AND schema_name = '{schema}' "
        f"AND table_name = '{name}'"
    )


def build_show_row_filter_sql(*, catalog: str, schema: str, name: str) -> str:
    return f"SHOW ROW FILTER ON {quote_fqn(catalog, schema, name)}"


# --- Parsers for capture output ---

def parse_show_grants_rows(rows: list[dict]) -> list[GrantEntry]:
    return [
        GrantEntry(
            principal=r["principal"],
            privilege=r.get("action_type") or r.get("privilege") or r.get("action"),
            object_type=r.get("object_type", "TABLE"),
        )
        for r in rows
    ]


def parse_show_tags_rows(rows: list[dict]) -> list[TagEntry]:
    return [TagEntry(name=r["tag_name"], value=r["tag_value"]) for r in rows]


# --- SQL builders for replay ---

def _sql_quote_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def build_replay_grants_sql(
    *, catalog: str, schema: str, name: str, grants: list[GrantEntry],
    object_type: str = "TABLE",
) -> list[str]:
    fqn = quote_fqn(catalog, schema, name)
    kw = _object_keyword(object_type)
    return [
        f"GRANT {g.privilege} ON {kw} {fqn} TO {quote_ident(g.principal)}"
        for g in grants
    ]


def build_replay_owner_sql(
    *, catalog: str, schema: str, name: str, owner: str, object_type: str = "TABLE",
) -> str:
    kw = _object_keyword(object_type)
    return f"ALTER {kw} {quote_fqn(catalog, schema, name)} OWNER TO {quote_ident(owner)}"


def build_replay_tags_sql(
    *, catalog: str, schema: str, name: str, tags: list[TagEntry],
) -> Optional[str]:
    if not tags:
        return None
    parts = ", ".join(
        f"{_sql_quote_literal(t.name)} = {_sql_quote_literal(t.value)}" for t in tags
    )
    return f"ALTER TABLE {quote_fqn(catalog, schema, name)} SET TAGS ({parts})"


def build_replay_comment_sql(
    *, catalog: str, schema: str, name: str, comment: str, object_type: str = "TABLE",
) -> str:
    kw = _object_keyword(object_type)
    return f"COMMENT ON {kw} {quote_fqn(catalog, schema, name)} IS {_sql_quote_literal(comment)}"
