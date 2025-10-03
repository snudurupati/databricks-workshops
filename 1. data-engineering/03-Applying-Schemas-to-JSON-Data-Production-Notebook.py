# Databricks notebook source
# COMMAND ----------

# Production-ready ingestion of ZIPs and Smartphone logs with explicit schemas and Delta writes.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    DoubleType,
)

# COMMAND ----------

# Define explicit schemas
zips_schema = StructType([
    StructField("city", StringType(), True),
    StructField("loc", ArrayType(DoubleType(), True), True),
    StructField("pop", IntegerType(), True),
])

sms_struct = StructType([
    StructField("Address", StringType(), True),
    StructField("date", StringType(), True),
    StructField("metadata", StructType([
        StructField("name", StringType(), True)
    ]), True),
])

logs_schema = StructType([
    StructField("Application", StringType(), True),
    StructField("Bluetooth", StringType(), True),
    StructField("Call", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("SMS", sms_struct, True),
    StructField("WiFi", StringType(), True),
])

# COMMAND ----------

# Read and write ZIPs dataset with pruning and robust options
zips_df = (
    spark.read
        .option("mode", "PERMISSIVE")
        .schema(zips_schema)
        .json("/mnt/training/zips.json")
)

(
    zips_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("curated.zips")
)

# COMMAND ----------

# Read smartphone logs, handle corrupts, compact, and write to Delta
logs_path = "/mnt/training/UbiqLog4UCI/14_F/log*"

logs_df = (
    spark.read
        .option("mode", "PERMISSIVE")
        .option("multiLine", True)
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(logs_schema)
        .json(logs_path)
        .filter("_corrupt_record IS NULL")
)

compact_logs = logs_df.repartition(8)

(
    compact_logs.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("curated.smartphone_logs")
)

# COMMAND ----------

# Optional warmed benchmark: schema vs inference
def time_with_warmup(op):
    import time
    start_warm = time.time()
    op()
    end_warm = time.time()
    start = time.time()
    op()
    end = time.time()
    return (end_warm - start_warm), (end - start)

schema_warm, schema_timed = time_with_warmup(lambda: spark.read.schema(logs_schema).json(logs_path).count())
infer_warm, infer_timed = time_with_warmup(lambda: spark.read.json(logs_path).count())

print(f"Schema read warm {schema_warm:.3f}s, timed {schema_timed:.3f}s; Inference warm {infer_warm:.3f}s, timed {infer_timed:.3f}s")

# COMMAND ----------


