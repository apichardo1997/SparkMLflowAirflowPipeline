"""SparkSession factory + a small write helper."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def get_spark(config: dict) -> SparkSession:
    """Build a SparkSession from config. Delta support is added only on demand."""
    builder = SparkSession.builder.appName(config["spark"]["app_name"])

    for key, value in config.get("spark", {}).get("configs", {}).items():
        builder = builder.config(key, value)

    # Enable Delta Lake only when output_format == "delta" (needs delta-spark).
    if config.get("output_format", "parquet").lower() == "delta":
        builder = (
            builder.config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError as exc:  # pragma: no cover
            raise SystemExit(
                "output_format is 'delta' but delta-spark is not installed.\n"
                "Run: pip install delta-spark   (or set output_format: parquet)"
            ) from exc

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_df(
    df: DataFrame,
    path: str | Path,
    fmt: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """Write a DataFrame as parquet or delta, optionally partitioned."""
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.format("delta" if fmt.lower() == "delta" else "parquet").save(str(path))
