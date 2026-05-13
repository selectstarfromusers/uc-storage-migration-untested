"""Delta-backed state I/O for _migration_ops tables."""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, BooleanType, LongType,
    ArrayType, MapType,
)

from utils.discovery import ObjectRecord, Classification

INVENTORY_SCHEMA = StructType([
    StructField("catalog", StringType(), False),
    StructField("schema", StringType(), False),
    StructField("name", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("table_type", StringType(), True),
    StructField("data_source_format", StringType(), True),
    StructField("storage_path", StringType(), True),
    StructField("parent_managed_location", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("last_altered", TimestampType(), True),
    StructField("requires_pipeline_handling", BooleanType(), False),
    StructField("size_bytes", LongType(), True),
    StructField("tag_count", LongType(), True),
    StructField("grant_count", LongType(), True),
    StructField("has_row_filter", BooleanType(), True),
    StructField("has_column_mask", BooleanType(), True),
    StructField("classification", StringType(), False),
    StructField("captured_at", TimestampType(), False),
])


class InventoryWriter:
    """Convert ObjectRecord + classification tuples into a Spark DataFrame and write to Delta."""

    def __init__(self, *, spark: SparkSession):
        self._spark = spark

    def records_to_dataframe(
        self, records: Iterable[tuple[ObjectRecord, Classification]]
    ) -> DataFrame:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows = []
        for rec, classification in records:
            rows.append({
                **asdict(rec),
                "classification": classification,
                "captured_at": now,
            })
        return self._spark.createDataFrame(rows, schema=INVENTORY_SCHEMA)

    def overwrite_delta(self, df: DataFrame, *, table_name: str) -> None:
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table_name)


MIGRATION_LOG_SCHEMA = StructType([
    StructField("catalog", StringType(), False),
    StructField("schema", StringType(), False),
    StructField("name", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("status", StringType(), False),
    StructField("claimed_by", StringType(), True),
    StructField("claimed_at", TimestampType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("finished_at", TimestampType(), True),
    StructField("row_count_before", LongType(), True),
    StructField("row_count_after", LongType(), True),
    StructField("schema_hash_before", StringType(), True),
    StructField("schema_hash_after", StringType(), True),
    StructField("staging_fqn", StringType(), True),
    StructField("pre_migration_fqn", StringType(), True),
    StructField("error_trace", StringType(), True),
    StructField("updated_at", TimestampType(), False),
])

OBJECT_METADATA_SNAPSHOT_SCHEMA = StructType([
    StructField("catalog", StringType(), False),
    StructField("schema", StringType(), False),
    StructField("name", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("snapshot_json", StringType(), False),
    StructField("captured_at", TimestampType(), False),
])

VALIDATION_RESULTS_SCHEMA = StructType([
    StructField("catalog", StringType(), False),
    StructField("schema", StringType(), False),
    StructField("name", StringType(), False),
    StructField("metadata_location_ok", BooleanType(), False),
    StructField("delta_log_at_new_ok", BooleanType(), True),
    StructField("input_file_name_ok", BooleanType(), False),
    StructField("parent_managed_location_match", BooleanType(), False),
    StructField("grants_ok", BooleanType(), True),
    StructField("owner_ok", BooleanType(), True),
    StructField("tags_ok", BooleanType(), True),
    StructField("row_filter_ok", BooleanType(), True),
    StructField("column_mask_ok", BooleanType(), True),
    StructField("comments_ok", BooleanType(), True),
    StructField("overall_pass", BooleanType(), False),
    StructField("evidence_json", StringType(), False),
    StructField("validated_at", TimestampType(), False),
])


def _now_naive_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class MigrationLog:
    """Writer + claim manager for _migration_ops.migration_log."""

    def __init__(self, *, spark: SparkSession, table_name: str):
        self._spark = spark
        self._table = table_name

    def ensure_exists(self) -> None:
        self._spark.createDataFrame([], schema=MIGRATION_LOG_SCHEMA).write.format("delta").mode(
            "ignore"
        ).saveAsTable(self._table)

    def claim(self, *, catalog: str, schema: str, name: str, object_type: str, actor: str) -> bool:
        """Atomically claim a row for migration. Returns True if claimed, False if already claimed by someone else."""
        now = _now_naive_utc()

        candidate = self._spark.createDataFrame(
            [(catalog, schema, name, object_type, "claimed", actor, now, now,
              None, None, None, None, None, None, None, None, now)],
            schema=MIGRATION_LOG_SCHEMA,
        )
        candidate.createOrReplaceTempView("_mig_log_candidate")

        self._spark.sql(f"""
            MERGE INTO {self._table} AS t
            USING _mig_log_candidate AS s
              ON t.catalog = s.catalog AND t.schema = s.schema AND t.name = s.name
            WHEN NOT MATCHED THEN INSERT *
        """)

        rows = self._spark.sql(
            f"SELECT claimed_by, status FROM {self._table} "
            f"WHERE catalog = '{catalog}' AND schema = '{schema}' AND name = '{name}'"
        ).collect()
        if not rows:
            return False
        return rows[0]["claimed_by"] == actor

    def update(self, *, catalog: str, schema: str, name: str, **fields) -> None:
        """Update fields for a claimed row."""
        sets = ", ".join(f"{k} = {self._to_sql_literal(v)}" for k, v in fields.items())
        sets += f", updated_at = {self._to_sql_literal(_now_naive_utc())}"
        self._spark.sql(
            f"UPDATE {self._table} SET {sets} "
            f"WHERE catalog = '{catalog}' AND schema = '{schema}' AND name = '{name}'"
        )

    @staticmethod
    def _to_sql_literal(v) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, int):
            return str(v)
        if isinstance(v, datetime):
            return f"TIMESTAMP '{v.isoformat()}'"
        s = str(v).replace("'", "''")
        return f"'{s}'"


class SnapshotWriter:
    """Persist GovernanceSnapshot dataclasses as JSON rows."""

    def __init__(self, *, spark: SparkSession, table_name: str):
        self._spark = spark
        self._table = table_name

    def ensure_exists(self) -> None:
        self._spark.createDataFrame([], schema=OBJECT_METADATA_SNAPSHOT_SCHEMA).write.format(
            "delta"
        ).mode("ignore").saveAsTable(self._table)

    def append(self, *, catalog: str, schema: str, name: str, object_type: str, snapshot_json: str) -> None:
        df = self._spark.createDataFrame(
            [(catalog, schema, name, object_type, snapshot_json, _now_naive_utc())],
            schema=OBJECT_METADATA_SNAPSHOT_SCHEMA,
        )
        df.write.format("delta").mode("append").saveAsTable(self._table)


class ValidationResultsWriter:
    def __init__(self, *, spark: SparkSession, table_name: str):
        self._spark = spark
        self._table = table_name

    def ensure_exists(self) -> None:
        self._spark.createDataFrame([], schema=VALIDATION_RESULTS_SCHEMA).write.format(
            "delta"
        ).mode("ignore").saveAsTable(self._table)
