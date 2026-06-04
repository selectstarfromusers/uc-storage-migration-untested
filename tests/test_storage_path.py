from unittest.mock import MagicMock

from utils.storage_path import resolve_storage_path


class _Row:
    def __init__(self, *vals):
        self._vals = vals

    def __iter__(self):
        return iter(self._vals)


def _spark_describing(*describe_rows):
    spark = MagicMock()
    result = MagicMock()
    result.collect.return_value = list(describe_rows)
    spark.sql.return_value = result
    return spark


def test_returns_info_schema_path_when_present():
    spark = MagicMock()
    out = resolve_storage_path(
        spark=spark,
        catalog="c", schema="s", name="t",
        info_schema_path="abfss://c@a.dfs.core.windows.net/x",
    )
    assert out == "abfss://c@a.dfs.core.windows.net/x"
    spark.sql.assert_not_called()


def test_falls_back_to_describe_extended_when_info_schema_null():
    spark = _spark_describing(
        _Row("col_name", "data_type", "comment"),
        _Row("id", "bigint", None),
        _Row("", "", None),
        _Row("# Detailed Table Information", "", None),
        _Row("Location", "abfss://c@old.dfs.core.windows.net/p", None),
        _Row("Provider", "delta", None),
    )
    out = resolve_storage_path(
        spark=spark, catalog="c", schema="s", name="t", info_schema_path=None,
    )
    assert out == "abfss://c@old.dfs.core.windows.net/p"
    spark.sql.assert_called_once()


def test_uses_volume_keyword_for_volumes():
    spark = _spark_describing(_Row("Location", "abfss://c@a.dfs.core.windows.net/v", None))
    resolve_storage_path(
        spark=spark, catalog="c", schema="s", name="v",
        info_schema_path=None, object_type="VOLUME",
    )
    call_args = spark.sql.call_args[0][0]
    # EXTENDED is invalid for volumes — the code uses "DESCRIBE VOLUME".
    assert "DESCRIBE VOLUME" in call_args
    assert "EXTENDED" not in call_args
    assert "`c`.`s`.`v`" in call_args


def test_returns_none_when_describe_raises():
    spark = MagicMock()
    spark.sql.side_effect = Exception("no permission")
    out = resolve_storage_path(
        spark=spark, catalog="c", schema="s", name="t", info_schema_path=None,
    )
    assert out is None
