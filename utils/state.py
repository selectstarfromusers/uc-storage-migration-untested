"""Delta-backed state I/O for _migration_ops tables."""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
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
        now = datetime.utcnow()
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
