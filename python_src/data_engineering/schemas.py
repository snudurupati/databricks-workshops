from typing import List

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    FloatType,
)


def get_zip_inferred_schema_example() -> StructType:
    """Placeholder returning empty schema; inference happens at runtime.

    This mirrors the Scala example where `spark.read.json(path)` infers schema.
    """
    return StructType([])


def get_zip_min_schema() -> StructType:
    """Return minimal schema for zips with city and population as IntegerType."""
    return StructType(
        [
            StructField("city", StringType(), True),
            StructField("pop", IntegerType(), True),
        ]
    )


def get_zip_full_schema() -> StructType:
    """Return schema for zips including city, loc (lat/long), and pop."""
    return StructType(
        [
            StructField("city", StringType(), True),
            StructField("loc", ArrayType(FloatType(), containsNull=True), True),
            StructField("pop", IntegerType(), True),
        ]
    )


