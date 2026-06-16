"""Four-layer evidence model for post-migration verification."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Protocol

from utils.paths import classify_url, parse_storage_url


def _is_on_account(url: Optional[str], expected_account: str) -> bool:
    """True iff `url` belongs to `expected_account`.

    Handles both bucket/account-name mode (`expected_account` is a bare
    name like "oldacct" or "mybucket") and prefix mode (`expected_account`
    contains a slash, e.g. "mybucket/migration-test-new"). The latter is
    used for AWS single-bucket testing where OLD/NEW are prefixes, not
    distinct buckets.
    """
    if not url:
        return False
    if "/" in expected_account:
        return classify_url(url, old="__unused__", new=expected_account) == "new"
    parsed = parse_storage_url(url)
    return parsed is not None and parsed.account == expected_account
from utils.sql import quote_fqn, parse_describe_extended_location
from utils.migration import compare_volume_content_hashes


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
    input_file_name_ok: Optional[bool]      # None if table was empty (no files to sample)
    parent_managed_location_match: Optional[bool]  # None if N/A (external object)
    content_checksum_ok: Optional[bool]            # None if N/A (disabled / external / no source)
    volume_content_checksum_ok: Optional[bool]     # None if N/A (non-volume / disabled / no source)
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
    """Extract bucket/account hosts from a list of storage URLs.

    Returns the canonical bucket/account names — callers in prefix-mode
    must compare using _is_on_account, not direct equality.
    """
    out: set[str] = set()
    for p in paths:
        parsed = parse_storage_url(p)
        if parsed:
            out.add(parsed.account)
    return out


_STORAGE_URL_PREFIXES = ("abfss://", "s3://", "s3a://", "s3n://")


def _parse_input_file_name_rows(rows) -> list[str]:
    out: list[str] = []
    for r in rows:
        d = r.asDict() if hasattr(r, "asDict") else dict(r)
        for v in d.values():
            if v and isinstance(v, str) and v.startswith(_STORAGE_URL_PREFIXES):
                out.append(v)
                break
    return out


def build_content_checksum_sql(fqn: str) -> str:
    """Order-independent, multiplicity-sensitive content fingerprint of a table.

    - ``xxhash64(*)`` hashes the typed columns positionally (NULL-safe, no
      CONCAT/delimiter ambiguity), 64-bit.
    - ``bit_xor`` is order-independent (file layout doesn't matter).
    - ``sum(... as decimal(38,0))`` is also order-independent but, unlike XOR,
      does NOT cancel even-count duplicate rows — so it catches row-multiplicity
      differences that ``bit_xor`` alone would miss. decimal(38,0) avoids overflow.
    - ``count(*)`` catches gross row-count differences.

    Two tables are considered identical iff all three of (n, xor64, sum64) match.
    `fqn` must already be quoted (use utils.sql.quote_fqn).
    """
    return (
        "SELECT count(*) AS n, "
        "coalesce(bit_xor(xxhash64(*)), 0) AS xor64, "
        "coalesce(sum(cast(xxhash64(*) AS decimal(38,0))), 0) AS sum64 "
        f"FROM {fqn}"
    )


def _checksum_row(spark: "_SqlExec", fqn: str) -> dict:
    rows = spark.sql(build_content_checksum_sql(fqn)).collect()
    r = rows[0]
    d = r.asDict() if hasattr(r, "asDict") else dict(r)
    # Normalize to comparable, JSON-serializable primitives. decimal/bigint → str.
    return {
        "n": int(d["n"]) if d.get("n") is not None else None,
        "xor64": str(d["xor64"]) if d.get("xor64") is not None else None,
        "sum64": str(d["sum64"]) if d.get("sum64") is not None else None,
    }


def compare_content_checksum(spark: "_SqlExec", *, source_fqn: str, target_fqn: str) -> tuple[bool, dict]:
    """Compute the content fingerprint of both tables and report whether they match.

    Returns (match, evidence). `source_fqn` is typically the retained
    ``<table>__pre_migration`` shadow; `target_fqn` is the migrated table.
    """
    src = _checksum_row(spark, source_fqn)
    tgt = _checksum_row(spark, target_fqn)
    match = (src["n"] == tgt["n"] and src["xor64"] == tgt["xor64"] and src["sum64"] == tgt["sum64"])
    return match, {
        "source_fqn": source_fqn, "target_fqn": target_fqn,
        "source": src, "target": tgt, "match": match,
    }


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
    is_external: bool = False,
    object_type: str = "TABLE",
    verify_content_checksum: bool = False,
    compare_fqn: Optional[str] = None,
    verify_volume_content_checksum: bool = False,
    volume_source_hashes: Optional[list] = None,
    volume_target_hashes: Optional[list] = None,
) -> ValidationResult:
    """Run the evidence layers against the migrated object and return a result.

    When `verify_content_checksum` is True and `compare_fqn` (the retained
    pre-migration source, already quoted) is provided for a managed object, a
    fifth layer compares a full-table content fingerprint of the migrated table
    against the source; a mismatch fails `overall_pass` (i.e. it blocks).
    """
    fqn = quote_fqn(catalog, schema, name)
    is_vol = (object_type or "").upper() == "VOLUME"
    evidence: dict = {}

    # --- Layer 1: location is on the new account ---
    # Volumes: DESCRIBE TABLE EXTENDED is invalid and you can't SELECT from one,
    # so read storage_location from information_schema.volumes. Tables: parse
    # DESCRIBE TABLE EXTENDED.
    try:
        if is_vol:
            vrows = spark.sql(
                "SELECT storage_location FROM system.information_schema.volumes "
                f"WHERE volume_catalog = '{catalog}' AND volume_schema = '{schema}' "
                f"AND volume_name = '{name}'"
            ).collect()
            location = (vrows[0].asDict() if hasattr(vrows[0], "asDict") else dict(vrows[0]))["storage_location"] if vrows else None
        else:
            rows = spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").collect()
            rendered = "\n".join(
                "\t".join(str(c) if c is not None else "" for c in (r.asDict().values() if hasattr(r, "asDict") else r))
                for r in rows
            )
            location = parse_describe_extended_location(rendered)
        evidence["describe_location"] = location
        metadata_ok = _is_on_account(location, expected_new_account)
    except Exception as e:
        metadata_ok = False
        evidence["describe_location_error"] = str(e)

    # --- Layer 2: _delta_log at new path (Delta only, external tables only) ---
    # For managed tables, UC blocks direct dbutils.fs.ls on __unitystorage paths
    # with "overlaps with managed storage". For managed tables the DESCRIBE
    # EXTENDED location (Layer 1) IS the Delta log location, so Layer 2 adds
    # no signal — mark N/A. For external tables, the vended-creds layer permits
    # ls and we can confirm _delta_log/ exists.
    delta_log_ok: Optional[bool]
    if is_delta and evidence.get("describe_location") and is_external:
        try:
            entries = fs.ls(f"{evidence['describe_location'].rstrip('/')}/_delta_log") or []
            delta_log_ok = bool(entries)
            evidence["delta_log_entries"] = len(entries)
        except Exception as e:
            delta_log_ok = False
            evidence["delta_log_error"] = str(e)
    else:
        delta_log_ok = None
        if is_delta and not is_external:
            evidence["delta_log_skipped"] = (
                "managed Delta — UC blocks direct __unitystorage ls. "
                "Layer 1 already proves location."
            )

    # --- Layer 3: file path via _metadata.file_path ---
    # input_file_name() is rejected in Unity Catalog with
    # UC_COMMAND_NOT_SUPPORTED; the supported path is _metadata.file_path.
    # Empty-table case: no rows means no file paths, but that does NOT prove
    # reads from old. Mark as None and exclude from overall_pass so empty
    # tables can still pass when other layers agree.
    input_ok: Optional[bool]
    if is_vol:
        input_ok = None  # N/A: can't SELECT _metadata from a volume
        evidence["input_file_name_skipped"] = "volume — no queryable rows"
    else:
        try:
            rows = spark.sql(
                f"SELECT _metadata.file_path AS path FROM {fqn} LIMIT {sample_limit}"
            ).collect()
            paths = _parse_input_file_name_rows(rows)
            hosts = _hosts_in_paths(paths)
            evidence["input_file_name_hosts"] = sorted(hosts)
            evidence["input_file_name_sample_count"] = len(paths)
            if not paths:
                input_ok = None
                evidence["input_file_name_empty"] = True
            else:
                # Every sampled path must be on expected_new_account.
                input_ok = bool(paths) and all(_is_on_account(p, expected_new_account) for p in paths)
        except Exception as e:
            input_ok = False
            evidence["input_file_name_error"] = str(e)

    # --- Layer 4: parent managed_location matches ---
    # For external objects the parent managed_location is informational only —
    # they live wherever their explicit storage_location points, regardless of
    # parent managed_location. Treat Layer 4 as N/A for externals.
    parent_ok: Optional[bool]
    if is_external:
        parent_ok = None
        evidence["parent_layer_skipped"] = "external object — Layer 4 N/A"
    elif parent_managed_location:
        parent_ok = _is_on_account(parent_managed_location, expected_new_account)
        parent_parsed = parse_storage_url(parent_managed_location)
        evidence["parent_account"] = parent_parsed.account if parent_parsed else None
    else:
        parent_ok = False
        evidence["parent_layer_no_location"] = True

    # --- Layer 5: full-table content checksum vs the pre-migration source ---
    # Gated (off unless enabled) and only meaningful for managed objects, which
    # retain a `__pre_migration` shadow to compare against. External objects are
    # dropped+recreated with no retained source, so there is nothing to diff.
    content_checksum_ok: Optional[bool]
    if verify_content_checksum and compare_fqn and not is_external and not is_vol:
        try:
            match, ev = compare_content_checksum(spark, source_fqn=compare_fqn, target_fqn=fqn)
            content_checksum_ok = match
            evidence["content_checksum"] = ev
        except Exception as e:
            content_checksum_ok = False
            evidence["content_checksum_error"] = str(e)
    else:
        content_checksum_ok = None
        if verify_content_checksum and is_vol:
            evidence["content_checksum_skipped"] = "volume — see volume_content_checksum (Layer 6); table fingerprint is N/A"
        elif verify_content_checksum and is_external:
            evidence["content_checksum_skipped"] = "external object — no __pre_migration source to compare"
        elif verify_content_checksum and not compare_fqn:
            evidence["content_checksum_skipped"] = "no compare_fqn provided"

    # --- Layer 6: per-file content hash for managed VOLUMES vs the shadow ---
    # The volume analogue of Layer 5. The expensive byte-reading + hashing is
    # done by the caller (04_validation, via Spark binaryFile / chunked FUSE
    # reads) and passed in as [(relpath, hash)] listings; this layer just
    # compares them. Managed volumes only — external volumes are
    # dropped+recreated with no retained shadow to diff.
    volume_content_checksum_ok: Optional[bool]
    if verify_volume_content_checksum and is_vol and not is_external \
            and volume_source_hashes is not None and volume_target_hashes is not None:
        try:
            match, ev = compare_volume_content_hashes(volume_source_hashes, volume_target_hashes)
            volume_content_checksum_ok = match
            evidence["volume_content_checksum"] = ev
        except Exception as e:
            volume_content_checksum_ok = False
            evidence["volume_content_checksum_error"] = str(e)
    else:
        volume_content_checksum_ok = None
        if verify_volume_content_checksum and is_vol and is_external:
            evidence["volume_content_checksum_skipped"] = "external volume — no __pre_migration shadow to compare"
        elif verify_volume_content_checksum and is_vol:
            evidence["volume_content_checksum_skipped"] = "no source/target hash listings provided"

    # overall_pass: every non-None layer must be True; None means N/A and is skipped.
    layer_results = [metadata_ok, delta_log_ok, input_ok, parent_ok,
                     content_checksum_ok, volume_content_checksum_ok]
    overall = all(lr is not False for lr in layer_results) and any(lr is True for lr in layer_results)

    return ValidationResult(
        catalog=catalog, schema=schema, name=name,
        metadata_location_ok=bool(metadata_ok),
        delta_log_at_new_ok=delta_log_ok,
        input_file_name_ok=input_ok,
        parent_managed_location_match=parent_ok,
        content_checksum_ok=content_checksum_ok,
        volume_content_checksum_ok=volume_content_checksum_ok,
        overall_pass=overall,
        evidence=evidence,
        validated_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )


def evidence_to_json(result: ValidationResult) -> str:
    """Serialize the evidence dict to JSON for the validation_results table."""
    return json.dumps(result.evidence, default=str, sort_keys=True)
