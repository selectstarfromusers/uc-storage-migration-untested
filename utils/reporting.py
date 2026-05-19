"""Decision-report logic and markdown rendering."""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from utils.discovery import ObjectRecord, Classification

Verdict = Literal[
    "ROLLBACK_FEASIBLE",
    "ROLLBACK_REQUIRES_SIGNOFF",
    "FORWARD_MIGRATE_REQUIRED",
]


@dataclass(frozen=True)
class DecisionThresholds:
    max_consistent_new_objects: int = 25
    max_bytes_on_new_gb: float = 10.0
    max_distinct_owners_on_new: int = 3
    max_age_days_on_new: int = 30


@dataclass(frozen=True)
class Recommendation:
    verdict: Verdict
    why: str
    new_object_count: int
    bytes_on_new: int


def compute_recommendation(
    classified: list[tuple[ObjectRecord, Classification]],
    *,
    thresholds: DecisionThresholds,
    bytes_on_new: int,
) -> Recommendation:
    new_records = [r for r, c in classified if c == "consistent_new"]
    n_new = len(new_records)
    bytes_gb = bytes_on_new / (1024 ** 3)
    owners = {r.owner for r in new_records if r.owner}
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    oldest_age_days = 0
    for r in new_records:
        if r.created_at:
            created = r.created_at.replace(tzinfo=None) if r.created_at.tzinfo else r.created_at
            oldest_age_days = max(oldest_age_days, (now - created).days)

    # Any object on new older than threshold → forward
    if oldest_age_days > thresholds.max_age_days_on_new:
        return Recommendation(
            verdict="FORWARD_MIGRATE_REQUIRED",
            why=(
                f"At least one new-storage object is {oldest_age_days} days old "
                f"(threshold {thresholds.max_age_days_on_new}). Rollback would "
                f"discard real workload history."
            ),
            new_object_count=n_new,
            bytes_on_new=bytes_on_new,
        )

    if (
        n_new > thresholds.max_consistent_new_objects
        or bytes_gb > thresholds.max_bytes_on_new_gb
        or len(owners) > thresholds.max_distinct_owners_on_new
    ):
        return Recommendation(
            verdict="FORWARD_MIGRATE_REQUIRED",
            why=(
                f"{n_new} objects, {bytes_gb:.1f} GB, {len(owners)} distinct owners on new "
                f"storage exceed rollback thresholds."
            ),
            new_object_count=n_new,
            bytes_on_new=bytes_on_new,
        )

    if n_new == 0:
        return Recommendation(
            verdict="ROLLBACK_FEASIBLE",
            why="No objects exist on new storage. Clean rollback path.",
            new_object_count=0,
            bytes_on_new=0,
        )

    return Recommendation(
        verdict="ROLLBACK_REQUIRES_SIGNOFF",
        why=(
            f"{n_new} new-storage objects within thresholds but non-zero. "
            f"Customer must confirm each one is throwaway before rollback drops them."
        ),
        new_object_count=n_new,
        bytes_on_new=bytes_on_new,
    )


_CLASSIFICATION_ORDER = [
    "consistent_old",
    "consistent_new",
    "drift_managed_on_old",
    "external_on_old",
    "external_on_new",
    "unknown_account",
    "path_missing",
]


def _size_coverage(records: list[tuple[ObjectRecord, Classification]]) -> dict:
    """Compute size-collection coverage stats for the summary block."""
    # Only count objects that COULD have a size (skip VIEWs / path_missing).
    sizable = [
        r for r, c in records
        if c != "path_missing" and r.table_type != "VIEW"
    ]
    sized = [r for r in sizable if r.size_bytes is not None]
    total_bytes = sum(r.size_bytes or 0 for r in sized)
    pct_missing = (
        100.0 * (len(sizable) - len(sized)) / len(sizable) if sizable else 0.0
    )
    # By object_type
    by_type = defaultdict(lambda: [0, 0])  # [total, sized]
    for r in sizable:
        by_type[r.object_type][0] += 1
        if r.size_bytes is not None:
            by_type[r.object_type][1] += 1
    return {
        "sized": len(sized),
        "total_sizable": len(sizable),
        "pct_missing": pct_missing,
        "total_bytes": total_bytes,
        "by_object_type": dict(by_type),
    }


def render_summary_markdown(
    *,
    records: list[tuple[ObjectRecord, Classification]],
    recommendation: Recommendation,
) -> str:
    counts: Counter = Counter(c for _, c in records)
    coverage = _size_coverage(records)

    # Per-catalog classification breakdown
    per_catalog: dict[str, Counter] = defaultdict(Counter)
    for r, c in records:
        per_catalog[r.catalog][c] += 1

    # Top catalogs by drift count
    drift_by_catalog = sorted(
        ((cat, ctr.get("drift_managed_on_old", 0)) for cat, ctr in per_catalog.items()),
        key=lambda x: x[1], reverse=True,
    )[:10]

    # Pipeline-handling objects (MVs / streaming tables)
    pipeline_objs = [r for r, _ in records if r.requires_pipeline_handling]

    # unknown_account objects need human review
    unknown_objs = [r for r, c in records if c == "unknown_account"]

    lines = ["## Inventory summary", "", "| Classification | Count |", "|---|---:|"]
    for cls in _CLASSIFICATION_ORDER:
        lines.append(f"| {cls} | {counts.get(cls, 0)} |")

    lines += ["", "## Per-catalog breakdown", ""]
    header = "| Catalog | " + " | ".join(_CLASSIFICATION_ORDER) + " |"
    sep = "|---|" + "---:|" * len(_CLASSIFICATION_ORDER)
    lines.append(header)
    lines.append(sep)
    for catalog in sorted(per_catalog):
        row = "| " + catalog + " | " + " | ".join(
            str(per_catalog[catalog].get(c, 0)) for c in _CLASSIFICATION_ORDER
        ) + " |"
        lines.append(row)

    lines += ["", "## Top catalogs by drift_managed_on_old", ""]
    if drift_by_catalog and drift_by_catalog[0][1] > 0:
        lines += ["| Catalog | drift count |", "|---|---:|"]
        for cat, cnt in drift_by_catalog:
            if cnt > 0:
                lines.append(f"| {cat} | {cnt} |")
    else:
        lines.append("_No drift objects found._")

    lines += ["", "## Pipeline-handling objects (MV / streaming tables)", ""]
    if pipeline_objs:
        lines += [
            f"{len(pipeline_objs)} object(s) flagged as requiring pipeline-owner coordination.",
            "These are NOT auto-migrated; pipeline owners must do a full refresh after upstream migration.",
            "",
            "| FQN | table_type |",
            "|---|---|",
        ]
        for r in pipeline_objs[:50]:
            lines.append(f"| `{r.catalog}.{r.schema}.{r.name}` | {r.table_type} |")
        if len(pipeline_objs) > 50:
            lines.append(f"_(+ {len(pipeline_objs) - 50} more)_")
    else:
        lines.append("_None._")

    lines += ["", "## unknown_account objects (needs human review)", ""]
    if unknown_objs:
        lines += ["| FQN | storage_path |", "|---|---|"]
        for r in unknown_objs[:50]:
            lines.append(f"| `{r.catalog}.{r.schema}.{r.name}` | {r.storage_path} |")
        if len(unknown_objs) > 50:
            lines.append(f"_(+ {len(unknown_objs) - 50} more)_")
    else:
        lines.append("_None._")

    lines += [
        "",
        "## Recommendation",
        "",
        f"**Verdict:** `{recommendation.verdict}`",
        "",
        f"{recommendation.why}",
        "",
        f"New-storage objects: {recommendation.new_object_count}, "
        f"bytes_on_new: {recommendation.bytes_on_new}",
        "",
        "## Size coverage",
        "",
        (f"**Sized:** {coverage['sized']} of {coverage['total_sizable']} objects "
         f"({coverage['pct_missing']:.1f}% missing). Total measured bytes: "
         f"{coverage['total_bytes']:,}."),
    ]
    if coverage["pct_missing"] > 0:
        lines.append("")
        lines.append(
            "_byte totals (bytes_on_new, drift_bytes) are **lower bounds** — "
            "they exclude objects whose size couldn't be measured (typically "
            "external tables, non-Delta formats, and unreachable volumes). "
            "Treat the cost / time estimate as a floor, not a quote._"
        )
    if coverage["by_object_type"]:
        lines += [
            "",
            "| Object type | sized / total |",
            "|---|---|",
        ]
        for obj_type, (total, sized) in sorted(coverage["by_object_type"].items()):
            lines.append(f"| {obj_type} | {sized} / {total} |")
    return "\n".join(lines)
