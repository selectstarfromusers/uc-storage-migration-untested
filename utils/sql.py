"""SQL identifier quoting and DESCRIBE output parsing."""
from __future__ import annotations

import re
from typing import Optional


def quote_ident(name: str) -> str:
    """Backtick-quote an SQL identifier, escaping internal backticks."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def quote_fqn(*parts: str) -> str:
    """Backtick-quote each part of a multi-part identifier and join with dots."""
    return ".".join(quote_ident(p) for p in parts)


_LOCATION_RE = re.compile(r"^\s*Location\s*[\t ]+(\S.*?)\s*$", re.MULTILINE)


def parse_describe_extended_location(output: str) -> Optional[str]:
    """Extract the Location: value from a DESCRIBE EXTENDED result string."""
    match = _LOCATION_RE.search(output)
    if not match:
        return None
    return match.group(1).strip()
