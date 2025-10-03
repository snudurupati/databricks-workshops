// Databricks notebook source
// MAGIC %md
// MAGIC Production-ready ingestion of ZIPs and Smartphone logs with explicit schemas and Delta writes.

// COMMAND ----------

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md
// MAGIC Define explicit schemas

// COMMAND ----------

val zipsSchema = StructType(List(
  StructField("city", StringType, true),
  StructField("loc", ArrayType(DoubleType, true), true),
  StructField("pop", IntegerType, true)
))

val smsStruct = StructType(List(
  StructField("Address", StringType, true),
  StructField("date", StringType, true),
  StructField("metadata", StructType(List(
    StructField("name", StringType, true)
  )), true)
))

val logsSchema = StructType(List(
  StructField("Application", StringType, true),
  StructField("Bluetooth", StringType, true),
  StructField("Call", StringType, true),
  StructField("Location", StringType, true),
  StructField("SMS", smsStruct, true),
  StructField("WiFi", StringType, true)
))

// COMMAND ----------

// MAGIC %md
// MAGIC Read and write ZIPs dataset with pruning and robust options

// COMMAND ----------

val zipsDF = spark.read
  .option("mode", "PERMISSIVE")
  .schema(zipsSchema)
  .json("/mnt/training/zips.json")

zipsDF.write.format("delta").mode("overwrite").saveAsTable("curated.zips")

// COMMAND ----------

// MAGIC %md
// MAGIC Read smartphone logs, handle corrupts, compact, and write to Delta

// COMMAND ----------

val logsPath = "/mnt/training/UbiqLog4UCI/14_F/log*"

val logsDF = spark.read
  .option("mode", "PERMISSIVE")
  .option("multiLine", true)
  .option("columnNameOfCorruptRecord", "_corrupt_record")
  .schema(logsSchema)
  .json(logsPath)
  .filter("_corrupt_record IS NULL")

val compactLogs = logsDF.repartition(8)

compactLogs.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("curated.smartphone_logs")

// COMMAND ----------

// MAGIC %md
// MAGIC Optional warmed benchmark: schema vs inference

// COMMAND ----------

def timeWithWarmup[T](op: => T): (Float, Float) = {
  val startWarm = System.currentTimeMillis
  op
  val endWarm = System.currentTimeMillis
  val start = System.currentTimeMillis
  op
  val end = System.currentTimeMillis
  ((endWarm - startWarm) / 1000f, (end - start) / 1000f)
}

val (schemaWarm, schemaTimed) = timeWithWarmup({
  spark.read.schema(logsSchema).json(logsPath).count()
})
val (inferWarm, inferTimed) = timeWithWarmup({
  spark.read.json(logsPath).count()
})

println(s"Schema read warm ${schemaWarm}s, timed ${schemaTimed}s; Inference warm ${inferWarm}s, timed ${inferTimed}s")



