"""Pre-migration health probes."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol

from utils.uc_client import ExternalLocationRecord


@dataclass(frozen=True)
class PreflightResult:
    target_path: str
    external_location_name: Optional[str]
    new_path_exists: bool
    partition_check_ok: Optional[bool]


@dataclass(frozen=True)
class PartitionProbeResult:
    old_count: int
    new_count: int
    complete: bool


class _Fs(Protocol):
    def ls(self, path: str):  # pragma: no cover
        ...


def check_external_location_for(
    *, target_path: str, external_locations: list[ExternalLocationRecord]
) -> Optional[ExternalLocationRecord]:
    """Return the external location whose URL is a prefix of target_path, or None."""
    for el in external_locations:
        url = el.url.rstrip("/")
        if target_path == url or target_path.startswith(url + "/"):
            return el
    return None


def probe_path_exists(*, fs: _Fs, path: str) -> bool:
    """Return True if `fs.ls(path)` succeeds and returns at least one entry."""
    try:
        entries = fs.ls(path)
        return bool(entries)
    except Exception:
        return False


def probe_partition_completeness(
    *, fs: _Fs, old_path: str, new_path: str
) -> PartitionProbeResult:
    """Compare directory counts between old and new paths.

    For partitioned tables, expects directory entries named `col=value`. Returns
    complete=True iff new_count >= old_count and old_count > 0.
    """
    try:
        old_entries = fs.ls(old_path) or []
    except Exception:
        old_entries = []
    try:
        new_entries = fs.ls(new_path) or []
    except Exception:
        new_entries = []
    return PartitionProbeResult(
        old_count=len(old_entries),
        new_count=len(new_entries),
        complete=len(new_entries) >= len(old_entries) and len(old_entries) > 0,
    )
