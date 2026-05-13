"""Four-layer evidence model for post-migration verification."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Protocol

from utils.paths import parse_abfss_url
from utils.sql import quote_fqn, parse_describe_extended_location


@dataclass(frozen=True)
class EvidenceLayer:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class ValidationResult:
    catalog: str
    schema: str
    name: str
    metadata_location_ok: bool
    delta_log_at_new_ok: Optional[bool]
    input_file_name_ok: bool
    parent_managed_location_match: bool
    overall_pass: bool
    evidence: dict
    validated_at: datetime


class _SqlExec(Protocol):
    def sql(self, query: str):  # pragma: no cover
        ...


class _Fs(Protocol):
    def ls(self, path: str):  # pragma: no cover
        ...


def _hosts_in_paths(paths: list[str]) -> set[str]:
    out: set[str] = set()
    for p in paths:
        parsed = parse_abfss_url(p)
        if parsed:
            out.add(parsed.account)
    return out


def _parse_input_file_name_rows(rows) -> list[str]:
    out: list[str] = []
    for r in rows:
        d = r.asDict() if hasattr(r, "asDict") else dict(r)
        for v in d.values():
            if v and isinstance(v, str) and v.startswith("abfss://"):
                out.append(v)
                break
    return out


def validate_object_on_new(
    *,
    spark: _SqlExec,
    fs: _Fs,
    catalog: str,
    schema: str,
    name: str,
    expected_new_account: str,
    parent_managed_location: Optional[str],
    is_delta: bool,
    sample_limit: int = 10000,
) -> ValidationResult:
    """Run all four evidence layers against the migrated object and return a result."""
    fqn = quote_fqn(catalog, schema, name)
    evidence: dict = {}

    # --- Layer 1: DESCRIBE EXTENDED → Location ---
    try:
        rows = spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").collect()
        rendered = "\n".join(
            "\t".join(str(c) if c is not None else "" for c in (r.asDict().values() if hasattr(r, "asDict") else r))
            for r in rows
        )
        location = parse_describe_extended_location(rendered)
        evidence["describe_location"] = location
        parsed = parse_abfss_url(location) if location else None
        metadata_ok = parsed is not None and parsed.account == expected_new_account
    except Exception as e:
        metadata_ok = False
        evidence["describe_location_error"] = str(e)

    # --- Layer 2: _delta_log at new path (Delta only) ---
    delta_log_ok: Optional[bool]
    if is_delta and evidence.get("describe_location"):
        try:
            entries = fs.ls(f"{evidence['describe_location'].rstrip('/')}/_delta_log") or []
            delta_log_ok = bool(entries)
            evidence["delta_log_entries"] = len(entries)
        except Exception as e:
            delta_log_ok = False
            evidence["delta_log_error"] = str(e)
    else:
        delta_log_ok = None

    # --- Layer 3: input_file_name() at runtime ---
    try:
        rows = spark.sql(
            f"SELECT input_file_name() AS path FROM {fqn} LIMIT {sample_limit}"
        ).collect()
        paths = _parse_input_file_name_rows(rows)
        hosts = _hosts_in_paths(paths)
        input_ok = bool(hosts) and hosts == {expected_new_account}
        evidence["input_file_name_hosts"] = sorted(hosts)
        evidence["input_file_name_sample_count"] = len(paths)
    except Exception as e:
        input_ok = False
        evidence["input_file_name_error"] = str(e)

    # --- Layer 4: parent managed_location matches ---
    parent_ok = False
    if parent_managed_location:
        parent_parsed = parse_abfss_url(parent_managed_location)
        parent_ok = parent_parsed is not None and parent_parsed.account == expected_new_account
        evidence["parent_account"] = parent_parsed.account if parent_parsed else None

    overall = bool(metadata_ok) and bool(input_ok) and bool(parent_ok) and (
        delta_log_ok is not False
    )

    return ValidationResult(
        catalog=catalog, schema=schema, name=name,
        metadata_location_ok=bool(metadata_ok),
        delta_log_at_new_ok=delta_log_ok,
        input_file_name_ok=bool(input_ok),
        parent_managed_location_match=bool(parent_ok),
        overall_pass=overall,
        evidence=evidence,
        validated_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )


def evidence_to_json(result: ValidationResult) -> str:
    """Serialize the evidence dict to JSON for the validation_results table."""
    return json.dumps(result.evidence, default=str, sort_keys=True)
