"""Data Formatting Layer (Formatted Zone).
Performs canonical, analysis-independent data cleansing per source system:
type casting, column standardization, and placeholder removal. Writes output
to the formatted zone as partitioned Parquet. Execution is idempotent."""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.checkpoints import zone_is_fresh
from src.utils.config import load_config, zone_path
from src.utils.spark_session import get_spark, write_df

DATASETS = ["income", "density", "price"]


def _format_income(df: DataFrame, config: dict) -> DataFrame:
    """Processes the RFD income index per neighborhood-year.
    Removes sentinel values representing missing data ('No consta' total row)
    and filters out records containing invalid numeric placeholders."""
    code = config["datasets"]["income"]["no_consta_code"]
    out = df.withColumn("rfd_index", F.col("rfd_raw").cast("double"))
    out = out.filter((F.col("Codi_Barri") != code) & F.col("rfd_index").isNotNull())
    return out.select(
        "Codi_Barri", "Nom_Barri", "Codi_Districte",
        F.col("Any").alias("year"),
        "rfd_index",
    )


def _format_density(df: DataFrame, config: dict) -> DataFrame:
    """Standardizes population, gross/net density, and surface area metrics per neighborhood-year."""
    return df.select(
        "Codi_Barri",
        F.col("Any").alias("year"),
        F.col("poblacio").cast("int").alias("population"),
        F.col("densitat_gross").cast("double").alias("density_gross"),
        F.col("densitat_net").cast("double").alias("density_net"),
        F.col("superficie").cast("double").alias("surface"),
    )


def _format_price(df: DataFrame, config: dict) -> DataFrame:
    """Flattens nested yearly price information into a structured tabular format.
    Note: The source key is non-standard; downstream integration layers resolve
    the official neighborhood code via string matching."""
    rec = df.select("neigh_name", F.explode("info").alias("rec"))
    out = rec.select(
        "neigh_name",
        F.col("rec.year").alias("year"),
        F.col("rec.PerMeter").cast("double").alias("PerMeter"),
    )
    return out.filter(F.col("year").isNotNull() & F.col("PerMeter").isNotNull())


_TRANSFORMS = {
    "income": _format_income,
    "density": _format_density,
    "price": _format_price,
}


def format_dataset(spark: SparkSession, config: dict, name: str, force: bool = False) -> None:
    """Cleanses a single ingested source dataset and writes it to the formatted zone.
    Data is partitioned by year. Employs a full overwrite strategy to ensure idempotency."""
    out = zone_path(config, "formatted") / name
    if zone_is_fresh(out) and not force:
        print(f"[A.4] {name}: formatted already fresh, skipping")
        return
    fmt = config["output_format"]
    landed = spark.read.format(fmt).load(str(zone_path(config, "landing") / name))
    cleaned = _TRANSFORMS[name](landed, config)
    write_df(cleaned, out, fmt=fmt, mode="overwrite", partition_by=["year"])
    print(f"[A.4] {name}: formatted {cleaned.count()} rows -> {out}")


def format_all(spark: SparkSession, config: dict, force: bool = False) -> None:
    for name in DATASETS:
        format_dataset(spark, config, name, force)


def main() -> None:
    parser = argparse.ArgumentParser(description="A.4 Formatted-zone cleaning")
    parser.add_argument("--dataset", choices=["all", *DATASETS], default="all")
    parser.add_argument("--force", action="store_true", help="re-format even if fresh")
    args = parser.parse_args()

    config = load_config()
    spark = get_spark(config)
    try:
        if args.dataset == "all":
            format_all(spark, config, args.force)
        else:
            format_dataset(spark, config, args.dataset, args.force)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
