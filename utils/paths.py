"""Storage URL parsing and account classification (ADLS abfss + AWS S3)."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Optional, Union

_ABFSS_RE = re.compile(
    r"^abfss://(?P<container>[^@]+)@(?P<account>[^.]+)\.dfs\.core\.windows\.net(?:/(?P<path>.*))?$",
    re.IGNORECASE,
)

# S3 / S3A / S3N — Databricks and Spark can emit any of these depending on
# cluster config. Bucket names are DNS-compliant, lower-case ASCII.
_S3_RE = re.compile(
    r"^s3[an]?://(?P<bucket>[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9])(?:/(?P<path>.*))?$",
    re.IGNORECASE,
)

AccountClass = Literal["old", "new", "other", "unknown"]
Scheme = Literal["abfss", "s3"]


@dataclass(frozen=True)
class AdlsPath:
    """abfss:// URL components.

    `account` is the storage-account name (the migratable unit). `container`
    is the container (filesystem) name. `path` is the object key, with no
    leading slash.
    """
    account: str
    container: str
    path: str
    raw: str
    scheme: str = "abfss"


@dataclass(frozen=True)
class S3Path:
    """s3:// URL components.

    `account` is the bucket name (the migratable unit on AWS). `container`
    is "" — S3 has no container/bucket distinction; the bucket *is* the
    container. We keep the field name parallel to AdlsPath so calling code
    can be scheme-agnostic. `path` is the object key, with no leading slash.
    """
    account: str
    container: str
    path: str
    raw: str
    scheme: str = "s3"


StoragePath = Union[AdlsPath, S3Path]


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


def parse_s3_url(url: Optional[str]) -> Optional[S3Path]:
    """Parse an s3://, s3a://, or s3n:// URL, or None if not S3.

    Returns S3Path with `account` = bucket name and `container` = "".
    """
    if not url:
        return None
    match = _S3_RE.match(url)
    if not match:
        return None
    return S3Path(
        account=match.group("bucket").lower(),
        container="",
        path=match.group("path") or "",
        raw=url,
    )


def parse_storage_url(url: Optional[str]) -> Optional[StoragePath]:
    """Scheme-agnostic parser. Returns an AdlsPath or S3Path, or None."""
    if not url:
        return None
    parsed: Optional[StoragePath] = parse_abfss_url(url)
    if parsed is not None:
        return parsed
    return parse_s3_url(url)


def classify_account(
    account: Optional[str], *, old: str, new: str
) -> AccountClass:
    """Classify a storage account / bucket name against the known old/new values."""
    if account is None:
        return "unknown"
    account_lower = account.lower()
    if account_lower == old.lower():
        return "old"
    if account_lower == new.lower():
        return "new"
    return "other"
