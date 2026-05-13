"""ADLS URL parsing and storage-account classification."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Optional

_ABFSS_RE = re.compile(
    r"^abfss://(?P<container>[^@]+)@(?P<account>[^.]+)\.dfs\.core\.windows\.net(?:/(?P<path>.*))?$",
    re.IGNORECASE,
)

AccountClass = Literal["old", "new", "other", "unknown"]


@dataclass(frozen=True)
class AdlsPath:
    account: str
    container: str
    path: str
    raw: str


def parse_abfss_url(url: Optional[str]) -> Optional[AdlsPath]:
    """Parse an abfss:// URL into its components, or None if not abfss."""
    if not url:
        return None
    match = _ABFSS_RE.match(url)
    if not match:
        return None
    return AdlsPath(
        account=match.group("account").lower(),
        container=match.group("container"),
        path=match.group("path") or "",
        raw=url,
    )


def classify_account(
    account: Optional[str], *, old: str, new: str
) -> AccountClass:
    """Classify a storage account name against the known old/new accounts."""
    if account is None:
        return "unknown"
    account_lower = account.lower()
    if account_lower == old.lower():
        return "old"
    if account_lower == new.lower():
        return "new"
    return "other"
