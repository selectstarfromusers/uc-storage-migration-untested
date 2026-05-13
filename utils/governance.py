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


# --- Execution classes for capture and replay ---

from typing import Protocol


class _SqlExec(Protocol):
    def sql(self, query: str):  # pragma: no cover
        ...


class GovernanceCapturer:
    """Capture governance state from a UC object into a GovernanceSnapshot."""

    def __init__(self, *, spark: _SqlExec):
        self._spark = spark

    def capture(
        self, *, catalog: str, schema: str, name: str, object_type: str = "TABLE",
    ) -> GovernanceSnapshot:
        grants = self._capture_grants(catalog, schema, name, object_type)
        owner = self._capture_owner(catalog, schema, name, object_type)
        tags = self._capture_tags(catalog, schema, name)
        row_filter_name, row_filter_cols = self._capture_row_filter(catalog, schema, name)
        column_masks = self._capture_column_masks(catalog, schema, name)
        table_comment, column_comments = self._capture_comments(catalog, schema, name, object_type)
        table_properties = self._capture_properties(catalog, schema, name, object_type)
        return GovernanceSnapshot(
            catalog=catalog, schema=schema, name=name,
            grants=grants, owner=owner, tags=tags,
            row_filter_name=row_filter_name,
            row_filter_using_columns=row_filter_cols,
            column_masks=column_masks,
            table_comment=table_comment,
            column_comments=column_comments,
            table_properties=table_properties,
        )

    def _rows(self, sql: str) -> list[dict]:
        try:
            return [r.asDict() for r in self._spark.sql(sql).collect()]
        except Exception:
            return []

    def _capture_grants(self, c, s, n, ot) -> list[GrantEntry]:
        return parse_show_grants_rows(self._rows(build_show_grants_sql(
            catalog=c, schema=s, name=n, object_type=ot,
        )))

    def _capture_tags(self, c, s, n) -> list[TagEntry]:
        return parse_show_tags_rows(self._rows(build_show_tags_sql(catalog=c, schema=s, name=n)))

    def _capture_owner(self, c, s, n, ot) -> Optional[str]:
        kw = _object_keyword(ot)
        rows = self._rows(f"DESCRIBE EXTENDED {kw} {quote_fqn(c, s, n)}")
        for r in rows:
            for k, v in r.items():
                if k.lower() in {"col_name", "info_name"} and str(v).lower() == "owner":
                    for vk, vv in r.items():
                        if vk != k:
                            return str(vv) if vv is not None else None
        return None

    def _capture_row_filter(self, c, s, n) -> tuple[Optional[str], list[str]]:
        rows = self._rows(build_show_row_filter_sql(catalog=c, schema=s, name=n))
        if not rows:
            return None, []
        r = rows[0]
        name = r.get("filter_name") or r.get("function_name")
        cols = r.get("using_columns") or r.get("input_columns") or []
        if isinstance(cols, str):
            cols = [c.strip() for c in cols.split(",") if c.strip()]
        return name, list(cols)

    def _capture_column_masks(self, c, s, n) -> list[ColumnMaskEntry]:
        rows = self._rows(
            "SELECT column_name, function_name, using_columns "
            "FROM system.information_schema.column_masks "
            f"WHERE catalog_name = '{c}' AND schema_name = '{s}' AND table_name = '{n}'"
        )
        out: list[ColumnMaskEntry] = []
        for r in rows:
            cols = r.get("using_columns") or []
            if isinstance(cols, str):
                cols = [c.strip() for c in cols.split(",") if c.strip()]
            out.append(ColumnMaskEntry(
                column=r["column_name"],
                mask_function=r["function_name"],
                using_columns=list(cols),
            ))
        return out

    def _capture_comments(self, c, s, n, ot) -> tuple[Optional[str], dict[str, str]]:
        rows = self._rows(f"DESCRIBE EXTENDED {_object_keyword(ot)} {quote_fqn(c, s, n)}")
        table_comment: Optional[str] = None
        column_comments: dict[str, str] = {}
        in_columns = True
        for r in rows:
            vals = list(r.values())
            if len(vals) < 2:
                continue
            label = str(vals[0]) if vals[0] is not None else ""
            if label.startswith("#") or label.startswith("Detailed"):
                in_columns = False
            if in_columns and vals[0] and len(vals) >= 3 and vals[2] is not None:
                column_comments[str(vals[0])] = str(vals[2])
            if not in_columns and label.lower() == "comment":
                table_comment = str(vals[1]) if vals[1] is not None else None
        return table_comment, column_comments

    def _capture_properties(self, c, s, n, ot) -> dict[str, str]:
        rows = self._rows(f"SHOW TBLPROPERTIES {quote_fqn(c, s, n)}")
        return {r["key"]: r["value"] for r in rows if "key" in r and "value" in r}


class GovernanceReplayer:
    """Replay a GovernanceSnapshot onto a UC object (typically the renamed-clone)."""

    def __init__(self, *, spark: _SqlExec):
        self._spark = spark

    def replay(self, snap: GovernanceSnapshot, *, target_fqn: tuple[str, str, str],
               object_type: str = "TABLE") -> list[str]:
        """Execute every replay statement; return a list of warnings (empty if clean)."""
        c, s, n = target_fqn
        warnings: list[str] = []

        for sql in build_replay_grants_sql(catalog=c, schema=s, name=n, grants=snap.grants, object_type=object_type):
            try:
                self._spark.sql(sql)
            except Exception as e:
                warnings.append(f"grant replay failed: {sql}: {e}")

        if snap.owner:
            try:
                self._spark.sql(build_replay_owner_sql(
                    catalog=c, schema=s, name=n, owner=snap.owner, object_type=object_type,
                ))
            except Exception as e:
                warnings.append(f"owner replay failed (principal may no longer exist): {snap.owner}: {e}")

        tag_sql = build_replay_tags_sql(catalog=c, schema=s, name=n, tags=snap.tags)
        if tag_sql:
            try:
                self._spark.sql(tag_sql)
            except Exception as e:
                warnings.append(f"tag replay failed: {e}")

        if snap.table_comment:
            try:
                self._spark.sql(build_replay_comment_sql(
                    catalog=c, schema=s, name=n, comment=snap.table_comment, object_type=object_type,
                ))
            except Exception as e:
                warnings.append(f"comment replay failed: {e}")

        for k, v in snap.table_properties.items():
            if k.startswith("delta."):
                continue  # Delta-managed properties, skip
            try:
                self._spark.sql(
                    f"ALTER TABLE {quote_fqn(c, s, n)} "
                    f"SET TBLPROPERTIES ({_sql_quote_literal(k)} = {_sql_quote_literal(v)})"
                )
            except Exception as e:
                warnings.append(f"property {k} replay failed: {e}")

        if snap.row_filter_name:
            warnings.append(
                f"row filter '{snap.row_filter_name}' was captured but auto-replay is not implemented; "
                f"manually run: ALTER TABLE {quote_fqn(c, s, n)} SET ROW FILTER ..."
            )
        if snap.column_masks:
            warnings.append(
                f"{len(snap.column_masks)} column mask(s) captured but auto-replay is not implemented; "
                f"manually reattach via ALTER TABLE ... ALTER COLUMN ... SET MASK ..."
            )

        return warnings
