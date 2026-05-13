"""Shared pytest fixtures."""
import pytest


@pytest.fixture(scope="session")
def spark():
    """Local PySpark session with Delta enabled for state I/O tests."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("uc-migration-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
    )
    session = builder.getOrCreate()
    yield session
    session.stop()
