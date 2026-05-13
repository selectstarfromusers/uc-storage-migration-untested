"""Decision-report logic and markdown rendering."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
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
    now = datetime.utcnow()
    oldest_age_days = 0
    for r in new_records:
        if r.created_at:
            oldest_age_days = max(oldest_age_days, (now - r.created_at).days)

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


def render_summary_markdown(
    *,
    records: list[tuple[ObjectRecord, Classification]],
    recommendation: Recommendation,
) -> str:
    counts: Counter = Counter(c for _, c in records)
    lines = ["## Inventory summary", "", "| Classification | Count |", "|---|---:|"]
    for cls in [
        "consistent_old",
        "consistent_new",
        "drift_managed_on_old",
        "external_on_old",
        "external_on_new",
        "unknown_account",
        "path_missing",
    ]:
        lines.append(f"| {cls} | {counts.get(cls, 0)} |")

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
    ]
    return "\n".join(lines)
