"""A.3 Ingestion (Landing zone). Read each source with its explicit schema, add an
ingestion timestamp, persist to data/landing/<name>. No cleaning. Idempotent:
skips a source whose landing output already carries a _SUCCESS marker."""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.checkpoints import zone_is_fresh
from src.schemas import DENSITY_SCHEMA, INCOME_SCHEMA, PRICE_SCHEMA
from src.utils.config import load_config, source_path, zone_path
from src.utils.spark_session import get_spark, write_df

SOURCES = ["income", "density", "price"]

_SCHEMAS = {
    "income": INCOME_SCHEMA,
    "density": DENSITY_SCHEMA,
    "price": PRICE_SCHEMA,
}


def _read_raw(spark: SparkSession, config: dict, name: str) -> DataFrame:
    """Read one source with its explicit schema. format tag selects the reader."""
    spec = config["datasets"][name]
    path = str(source_path(config, spec["path_glob"]))
    fmt = spec["format"]
    schema = _SCHEMAS[name]
    if fmt == "csv":
        return spark.read.schema(schema).option("header", True).csv(path)
    if fmt == "json_array":
        return spark.read.schema(schema).option("multiLine", True).json(path)
    if fmt == "json_lines":
        return spark.read.schema(schema).json(path)
    raise ValueError(f"{name}: unknown format '{fmt}'")


def _to_landing(name: str, df: DataFrame) -> DataFrame:
    """Project to parquet-safe column names. No row filtering, no value cleaning."""
    if name == "income":
        return df
    if name == "density":
        return df.select(
            "Any", "Codi_Districte", "Nom_Districte", "Codi_Barri", "Nom_Barri",
            F.col("Població").alias("poblacio"),
            F.col("Superfície (ha)").alias("superficie"),
            F.col("Densitat (hab/ha)").alias("densitat_gross"),
            F.col("Densitat neta (hab/ha)").alias("densitat_net"),
        )
    if name == "price":
        # _id is a price-file row index, NOT the official Codi_Barri; kept raw as
        # price_id. Real code is resolved by neigh_name in Exploitation.
        return df.select(
            F.col("_id").alias("price_id"),
            F.col("neigh_name ").alias("neigh_name"),
            "district_id", "district_name", "info",
        )
    raise ValueError(f"unknown source '{name}'")


def ingest(spark: SparkSession, config: dict, name: str, force: bool = False) -> None:
    """Land one source as parquet with an _ingested_at load-time column. Incrementality:
    full OVERWRITE snapshot; the _SUCCESS marker drives the idempotent skip."""
    out = zone_path(config, "landing") / name
    if zone_is_fresh(out) and not force:
        print(f"[A.3] {name}: landing already fresh, skipping")
        return
    df = _to_landing(name, _read_raw(spark, config, name))
    df = df.withColumn("_ingested_at", F.current_timestamp())
    write_df(df, out, fmt=config["output_format"], mode="overwrite")
    print(f"[A.3] {name}: landed {df.count()} rows -> {out}")


def ingest_all(spark: SparkSession, config: dict, force: bool = False) -> None:
    for name in SOURCES:
        ingest(spark, config, name, force)


def main() -> None:
    parser = argparse.ArgumentParser(description="A.3 Landing-zone ingestion")
    parser.add_argument("--source", choices=["all", *SOURCES], default="all")
    parser.add_argument("--force", action="store_true", help="re-ingest even if fresh")
    args = parser.parse_args()

    config = load_config()
    spark = get_spark(config)
    try:
        if args.source == "all":
            ingest_all(spark, config, args.force)
        else:
            ingest(spark, config, args.source, args.force)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
