"""Tests for the dataset-independent helpers."""
from __future__ import annotations

import pytest

from src.checkpoints import data_fingerprint, zone_is_fresh
from src.quality import (
    DataQualityError,
    assert_columns,
    assert_min_classes,
    assert_no_nulls,
    assert_not_empty,
)


def test_zone_is_fresh(tmp_path):
    assert not zone_is_fresh(tmp_path)
    (tmp_path / "_SUCCESS").touch()
    assert zone_is_fresh(tmp_path)


def test_data_fingerprint_stable(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "label"])
    assert data_fingerprint(df) == data_fingerprint(df)


def test_data_fingerprint_changes_with_rows(spark):
    df1 = spark.createDataFrame([(1, "a")], ["id", "label"])
    df2 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "label"])
    assert data_fingerprint(df1) != data_fingerprint(df2)


def test_quality_gates(spark):
    df = spark.createDataFrame([(1, "low"), (2, "high")], ["id", "target"])
    assert_not_empty(df, "t")
    assert_columns(df, ["id", "target"], "t")
    assert_no_nulls(df, ["id", "target"], "t")
    assert_min_classes(df, "target", 2)


def test_quality_detects_nulls(spark):
    df = spark.createDataFrame([(1, "low"), (2, None)], ["id", "target"])
    with pytest.raises(DataQualityError):
        assert_no_nulls(df, ["target"], "t")


def test_quality_detects_single_class(spark):
    df = spark.createDataFrame([(1, "low"), (2, "low")], ["id", "target"])
    with pytest.raises(DataQualityError):
        assert_min_classes(df, "target", 2)
