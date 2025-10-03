from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def read_json(
    spark: SparkSession, path: str, schema: Optional[StructType] = None
) -> DataFrame:
    reader = spark.read
    if schema is not None and len(schema) > 0:
        reader = reader.schema(schema)
    return reader.json(path)


def display_df(df: DataFrame) -> None:
    # Helper to avoid Databricks `display` dependency in pure Python.
    df.show(20, truncate=False)


