"""A.4 Formatting (Formatted zone). Canonical, analysis-independent cleaning per
source: cast types, standardize column names to the goal contract, drop placeholder
rows. Writes data/formatted/<name> as Parquet partitioned by year. Idempotent."""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.checkpoints import zone_is_fresh
from src.utils.config import load_config, zone_path
from src.utils.spark_session import get_spark, write_df

DATASETS = ["income", "density", "price"]


def _format_income(df: DataFrame, config: dict) -> DataFrame:
    """RFD income index per neighborhood-year. Drops the 'No consta' total row
    (configured Codi_Barri sentinel) and rows whose RFD placeholder ('-') fails
    the numeric cast."""
    code = config["datasets"]["income"]["no_consta_code"]
    out = df.withColumn("rfd_index", F.col("rfd_raw").cast("double"))
    out = out.filter((F.col("Codi_Barri") != code) & F.col("rfd_index").isNotNull())
    return out.select(
        "Codi_Barri", "Nom_Barri", "Codi_Districte",
        F.col("Any").alias("year"),
        "rfd_index",
    )


def _format_density(df: DataFrame, config: dict) -> DataFrame:
    """Population, gross/net density and surface per neighborhood-year."""
    return df.select(
        "Codi_Barri",
        F.col("Any").alias("year"),
        F.col("poblacio").cast("int").alias("population"),
        F.col("densitat_gross").cast("double").alias("density_gross"),
        F.col("densitat_net").cast("double").alias("density_net"),
        F.col("superficie").cast("double").alias("surface"),
    )


def _format_price(df: DataFrame, config: dict) -> DataFrame:
    """Flatten the nested per-year info[] to (neigh_name, year, PerMeter). Keyed by
    name: price_id is not the official code, so Codi_Barri is resolved by name in A.5."""
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
    """Clean one landed source and write it partitioned by year. Incrementality: full OVERWRITE."""
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
