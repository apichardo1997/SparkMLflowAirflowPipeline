"""Explicit read schemas for the three Landing-zone sources. No inferSchema."""
from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# income CSVs, header order: Any, Codi_Districte, Nom_Districte, Codi_Barri,
# Nom_Barri, Població, "Índex RFD Barcelona = 100". Applied by position (header
# skipped). RFD kept raw (string): "No consta" rows carry "-"; cast in Formatted.
INCOME_SCHEMA = StructType([
    StructField("Any", IntegerType(), True),
    StructField("Codi_Districte", IntegerType(), True),
    StructField("Nom_Districte", StringType(), True),
    StructField("Codi_Barri", IntegerType(), True),
    StructField("Nom_Barri", StringType(), True),
    StructField("poblacio", IntegerType(), True),
    StructField("rfd_raw", StringType(), True),
])

# density JSON array (multiLine). Field names match JSON keys exactly, including
# accents. "Superfície Residencial (ha)" omitted (unused); extra JSON keys ignored.
DENSITY_SCHEMA = StructType([
    StructField("Any", IntegerType(), True),
    StructField("Codi_Districte", IntegerType(), True),
    StructField("Nom_Districte", StringType(), True),
    StructField("Codi_Barri", IntegerType(), True),
    StructField("Nom_Barri", StringType(), True),
    StructField("Població", IntegerType(), True),
    StructField("Superfície (ha)", DoubleType(), True),
    StructField("Densitat (hab/ha)", DoubleType(), True),
    StructField("Densitat neta (hab/ha)", DoubleType(), True),
])

# price JSON Lines (one object per line, no multiLine). "neigh_name " key carries
# a trailing space. Only year + PerMeter retained per nested info[] record.
PRICE_INFO_SCHEMA = ArrayType(StructType([
    StructField("year", IntegerType(), True),
    StructField("PerMeter", DoubleType(), True),
]))

PRICE_SCHEMA = StructType([
    StructField("_id", IntegerType(), True),
    StructField("neigh_name ", StringType(), True),
    StructField("district_id", IntegerType(), True),
    StructField("district_name", StringType(), True),
    StructField("info", PRICE_INFO_SCHEMA, True),
])
