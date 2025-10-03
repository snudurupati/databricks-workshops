// Production-ready Spark job for applying schemas to JSON data and writing curated outputs.
// Key choices:
// - Avoid schema inference on JSON to skip the extra full scan.
// - Explicit corrupt-record handling (PERMISSIVE with _corrupt_record filtering) for data quality.
// - multiLine enabled for logs that may contain multi-line JSON entries.
// - Mitigate many-small-files via controlled repartitioning before Delta writes.
// - Simple warmed benchmarking to compare schema vs inference (observability only).

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object ApplySchemasJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism.toString)

    // Ingest zips with explicit schema and column pruning
    val zipsDF = readZips(spark)
    zipsDF.write.format("delta").mode("overwrite").saveAsTable("curated.zips")

    // Ingest smartphone logs with robust options, repartition, and Delta write
    val logsDF = readSmartphoneLogs(spark, "/mnt/training/UbiqLog4UCI/14_F/log*")
    val compactLogs = logsDF.repartition(8)
    compactLogs.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("curated.smartphone_logs")

    // Optional: simple warmed benchmarking comparing schema vs inference on logs
    val (schemaWarm, schemaTimed) = timeWithWarmup({
      spark.read.schema(logsSchema).json("/mnt/training/UbiqLog4UCI/14_F/log*").count()
    })
    val (inferWarm, inferTimed) = timeWithWarmup({
      spark.read.json("/mnt/training/UbiqLog4UCI/14_F/log*").count()
    })
    println(s"Schema read warm ${schemaWarm}s, timed ${schemaTimed}s; Inference warm ${inferWarm}s, timed ${inferTimed}s")
  }

  private def readZips(spark: SparkSession): DataFrame = {
    val zipsSchema = StructType(List(
      StructField("city", StringType, true),
      StructField("loc", ArrayType(DoubleType, true), true),
      StructField("pop", IntegerType, true)
    ))

    val df = spark.read
      .option("mode", "PERMISSIVE")
      .schema(zipsSchema)
      .json("/mnt/training/zips.json")

    // Column pruning already applied via schema; persist if reused downstream
    df
  }

  private def readSmartphoneLogs(spark: SparkSession, path: String): DataFrame = {
    // Define a targeted schema for the known categories. Adjust as needed.
    // Here we model the SMS nested struct minimally; expand per requirements.
    val smsStruct = StructType(List(
      StructField("Address", StringType, true),
      StructField("date", StringType, true),
      StructField("metadata", StructType(List(
        StructField("name", StringType, true)
      )), true)
    ))

    val root = StructType(List(
      StructField("Application", StringType, true),
      StructField("Bluetooth", StringType, true),
      StructField("Call", StringType, true),
      StructField("Location", StringType, true),
      StructField("SMS", smsStruct, true),
      StructField("WiFi", StringType, true)
    ))

    val df = spark.read
      .option("mode", "PERMISSIVE")
      .option("multiLine", true)
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .schema(root)
      .json(path)
      .filter("_corrupt_record IS NULL")

    df
  }

  // Expose logs schema for benchmarking
  private def logsSchema: StructType = {
    val smsStruct = StructType(List(
      StructField("Address", StringType, true),
      StructField("date", StringType, true),
      StructField("metadata", StructType(List(
        StructField("name", StringType, true)
      )), true)
    ))
    StructType(List(
      StructField("Application", StringType, true),
      StructField("Bluetooth", StringType, true),
      StructField("Call", StringType, true),
      StructField("Location", StringType, true),
      StructField("SMS", smsStruct, true),
      StructField("WiFi", StringType, true)
    ))
  }

  private def timeWithWarmup[T](op: => T): (Float, Float) = {
    val startWarm = System.currentTimeMillis
    op
    val endWarm = System.currentTimeMillis
    val start = System.currentTimeMillis
    op
    val end = System.currentTimeMillis
    ((endWarm - startWarm) / 1000f, (end - start) / 1000f)
  }
}


