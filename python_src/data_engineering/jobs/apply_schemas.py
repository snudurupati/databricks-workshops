"""Production-oriented job translating the Scala notebook 'Applying Schemas to JSON Data'.

The job demonstrates:
 - Schema inference
 - Applying user-defined schemas (primitive and composite types)

Paths using `/mnt/training` may be disabled in some workspaces. Provide an
alternative path via `--zips-path`.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

from data_engineering.schemas import (
    get_zip_min_schema,
    get_zip_full_schema,
)
from data_engineering.io import read_json, display_df


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--zips-path",
        type=str,
        required=True,
        help="Path to zips.json (e.g., dbfs:/databricks-datasets/samples/people/people.json equivalent or a UC volume)",
    )
    parser.add_argument("--show", action="store_true", help="Show results to stdout")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    spark = (
        SparkSession.builder.appName("apply-schemas-json").getOrCreate()
    )

    # Inferred schema example
    df_inferred = read_json(spark, args.zips_path)
    if args.show:
        df_inferred.printSchema()

    # Minimal schema (city, pop)
    min_schema = get_zip_min_schema()
    df_min = read_json(spark, args.zips_path, schema=min_schema)
    if args.show:
        display_df(df_min)

    # Full schema including loc array
    full_schema = get_zip_full_schema()
    df_full = read_json(spark, args.zips_path, schema=full_schema)
    if args.show:
        display_df(df_full)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))


