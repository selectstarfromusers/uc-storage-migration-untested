"""SQL query builders for downstream consumer discovery from system.access.*"""
from __future__ import annotations


def build_lineage_consumers_query(*, inventory_table: str, days: int) -> str:
    """Return SQL that finds upstream-to-in-scope-object lineage edges from the last N days."""
    return f"""
WITH inv AS (
  SELECT catalog, schema, name FROM {inventory_table}
  WHERE classification IN ('drift_managed_on_old', 'external_on_old')
)
SELECT
  l.source_table_full_name AS source,
  l.target_table_full_name AS target,
  l.event_time,
  l.entity_type,
  l.entity_id
FROM system.access.table_lineage l
JOIN inv i
  ON (l.source_table_catalog = i.catalog AND l.source_table_schema = i.schema AND l.source_table_name = i.name)
  OR (l.target_table_catalog = i.catalog AND l.target_table_schema = i.schema AND l.target_table_name = i.name)
WHERE l.event_time > current_timestamp() - INTERVAL {days} DAYS
""".strip()


def build_recent_writes_query(*, inventory_table: str, days: int) -> str:
    """Return SQL that finds recent write actions against in-scope tables from audit logs."""
    return f"""
WITH inv AS (
  SELECT catalog, schema, name FROM {inventory_table}
  WHERE classification IN ('drift_managed_on_old', 'external_on_old', 'consistent_new')
)
SELECT
  a.event_time,
  a.user_identity.email AS actor,
  a.action_name,
  a.request_params
FROM system.access.audit a
JOIN inv i
  ON a.request_params['full_name_arg'] = concat_ws('.', i.catalog, i.schema, i.name)
WHERE a.event_time > current_timestamp() - INTERVAL {days} DAYS
  AND a.action_name IN ('updateTable', 'createTable', 'mergeTable', 'writeTable')
""".strip()
