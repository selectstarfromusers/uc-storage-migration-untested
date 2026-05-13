from utils.lineage import build_lineage_consumers_query, build_recent_writes_query


def test_lineage_consumers_query_includes_inventory_join():
    sql = build_lineage_consumers_query(inventory_table="main._migration_ops.inventory", days=30)
    assert "system.access.table_lineage" in sql
    assert "main._migration_ops.inventory" in sql
    assert "INTERVAL 30 DAYS" in sql


def test_recent_writes_query_includes_audit():
    sql = build_recent_writes_query(inventory_table="main._migration_ops.inventory", days=30)
    assert "system.access.audit" in sql
    assert "main._migration_ops.inventory" in sql
    assert "INTERVAL 30 DAYS" in sql
