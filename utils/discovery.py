"""Classification of UC objects against old/new ADLS account state."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

from utils.paths import classify_account, classify_url, parse_storage_url

Classification = Literal[
    "consistent_old",
    "consistent_new",
    "drift_managed_on_old",
    "external_on_old",
    "external_on_new",
    "unknown_account",
    "path_missing",
]


@dataclass(frozen=True)
class ObjectRecord:
    catalog: str
    schema: str
    name: str
    object_type: str           # "TABLE" | "VOLUME" | "REGISTERED_MODEL" | "FUNCTION"
    table_type: Optional[str]  # "MANAGED" | "EXTERNAL" | "VIEW" | "MATERIALIZED_VIEW" | "STREAMING_TABLE"
    data_source_format: Optional[str]
    storage_path: Optional[str]
    parent_managed_location: Optional[str]
    owner: Optional[str]
    created_at: Optional[datetime]
    last_altered: Optional[datetime]
    requires_pipeline_handling: bool = False
    size_bytes: Optional[int] = None
    tag_count: Optional[int] = None
    grant_count: Optional[int] = None
    has_row_filter: Optional[bool] = None
    has_column_mask: Optional[bool] = None


def _requires_pipeline_handling(table_type: Optional[str]) -> bool:
    return table_type in {"MATERIALIZED_VIEW", "STREAMING_TABLE"}


def _account_class(url: Optional[str], *, old: str, new: str) -> str:
    return classify_url(url, old=old, new=new)


def classify_object(rec: ObjectRecord, *, old: str, new: str) -> Classification:
    """Classify an object based on its storage path vs its parent's managed location."""
    # Views and anything without a storage path → path_missing
    if rec.storage_path is None or rec.table_type in {"VIEW"}:
        return "path_missing"

    obj_cls = _account_class(rec.storage_path, old=old, new=new)
    parent_cls = _account_class(rec.parent_managed_location, old=old, new=new)

    if obj_cls == "other":
        return "unknown_account"

    is_managed = rec.table_type in {"MANAGED", "MATERIALIZED_VIEW", "STREAMING_TABLE"}

    if is_managed:
        if obj_cls == "old" and parent_cls == "old":
            return "consistent_old"
        if obj_cls == "new" and parent_cls == "new":
            return "consistent_new"
        if obj_cls == "old" and parent_cls == "new":
            return "drift_managed_on_old"
        # Managed but parent says old while object is on new — unusual but call it consistent_new for safety
        if obj_cls == "new" and parent_cls == "old":
            return "consistent_new"
        return "unknown_account"

    # External
    if obj_cls == "old":
        return "external_on_old"
    if obj_cls == "new":
        return "external_on_new"
    return "unknown_account"
