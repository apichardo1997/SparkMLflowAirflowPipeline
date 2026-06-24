"""Data-quality gates. Each check raises DataQualityError on failure."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    """Raised when a quality gate fails."""


def assert_not_empty(df: DataFrame, name: str) -> None:
    if df.head(1) == []:
        raise DataQualityError(f"{name}: table is empty")


def assert_columns(df: DataFrame, expected: list[str], name: str) -> None:
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise DataQualityError(f"{name}: missing columns {missing}")


def assert_no_nulls(df: DataFrame, columns: list[str], name: str) -> None:
    counts = df.select(
        [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns]
    ).collect()[0]
    offenders = {c: counts[c] for c in columns if counts[c]}
    if offenders:
        raise DataQualityError(f"{name}: null values in {offenders}")


def assert_min_classes(df: DataFrame, target: str, minimum: int = 2) -> None:
    """Target must have at least `minimum` distinct classes to train a classifier."""
    n = df.select(target).distinct().count()
    if n < minimum:
        raise DataQualityError(f"{target}: needs >={minimum} classes, found {n}")
