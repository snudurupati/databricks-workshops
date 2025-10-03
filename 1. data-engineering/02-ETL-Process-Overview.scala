// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # ETL Process Overview
// MAGIC
// MAGIC Apache Spark&trade; and Databricks&reg; allow you to create an end-to-end _extract, transform, load (ETL)_ pipeline.
// MAGIC ## In this lesson you:
// MAGIC * Create a basic end-to-end ETL pipeline
// MAGIC * Demonstrate the Spark approach to ETL pipelines
// MAGIC
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
// MAGIC
// MAGIC ## Prerequisites
// MAGIC * Web browser: Please use a <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers" target="_blank">supported browser</a>.
// MAGIC * Concept (optional): <a href="https://academy.databricks.com/collections/frontpage/products/dataframes" target="_blank">DataFrames course from Databricks Academy</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### The Spark Approach
// MAGIC
// MAGIC Spark offers a compute engine and connectors to virtually any data source. By leveraging easily scaled infrastructure and accessing data where it lives, Spark addresses the core needs of a big data application.
// MAGIC
// MAGIC These principles comprise the Spark approach to ETL, providing a unified and scalable approach to big data pipelines: <br><br>
// MAGIC
// MAGIC 1. Databricks and Spark offer a **unified platform** 
// MAGIC  - Spark on Databricks combines ETL, stream processing, machine learning, and collaborative notebooks.
// MAGIC  - Data scientists, analysts, and engineers can write Spark code in Python, Scala, SQL, and R.
// MAGIC 2. Spark's unified platform is **scalable to petabytes of data and clusters of thousands of nodes**.  
// MAGIC  - The same code written on smaller data sets scales to large workloads, often with only small changes.
// MAGIC 2. Spark on Databricks decouples data storage from the compute and query engine.  
// MAGIC  - Spark's query engine **connects to any number of data sources** such as S3, Azure Blob Storage, Redshift, and Kafka.  
// MAGIC  - This **minimizes costs**; a dedicated cluster does not need to be maintained and the compute cluster is **easily updated to the latest version** of Spark.
// MAGIC  
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/Workload_Tools_2-01.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

// COMMAND ----------

// MAGIC %md
// MAGIC ### A Basic ETL Job
// MAGIC
// MAGIC In this lesson you use web log files from the <a href="https://www.sec.gov/dera/data/edgar-log-file-data-set.html" target="_blank">US Securities and Exchange Commision website</a> to do a basic ETL for a day of server activity. You will extract the fields of interest and load them into persistent storage.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Getting Started
// MAGIC
// MAGIC Run the following cell to configure our "classroom."
// MAGIC
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Remember to attach your notebook to a cluster. Click <b>Detached</b> in the upper left hand corner and then select your preferred cluster.
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

// COMMAND ----------

// MAGIC %md
// MAGIC Run the cell below to mount the data.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC The Databricks File System (DBFS) is an HDFS-like interface to bulk data storages like Azure's Blob storage service.
// MAGIC
// MAGIC Pass the path `/mnt/training/EDGAR-Log-20170329/EDGAR-Log-20170329.csv` into `spark.read.csv`to access data stored in DBFS. Use the header option to specify that the first line of the file is the header.

// COMMAND ----------

//use DBUtils to sample the data
dbutils.fs.head("/mnt/training/EDGAR-Log-20170329/EDGAR-Log-20170329.csv")

// COMMAND ----------

// MAGIC %md Convert the data in text to Delta (optimized Parquet) format

// COMMAND ----------

val srcPath = "/mnt/training/EDGAR-Log-20170329/EDGAR-Log-20170329.csv"
val targetPath = "/tmp/"+username+"/delta/EDGAR-Log-20170329"

spark
  .read
    .option("header", true)
    .csv(srcPath)
    .sample(withReplacement=false, fraction=0.3, seed=3) // using a sample to reduce data size
  .write
    .format("delta")
    .mode("overwrite")
    .save(targetPath)

// COMMAND ----------

// MAGIC %md Create a DataFrame from the Delta data

// COMMAND ----------

val logDF = spark
              .read
              .format("delta")
              .load(targetPath)

display(logDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, review the server-side errors, which have error codes in the 500s.  

// COMMAND ----------

val serverErrorDF = logDF
  .filter(($"code" >= 500) && ($"code" < 600))
  .select("date", "time", "extention", "code")

display(serverErrorDF)

// COMMAND ----------

display(dbutils.fs.ls(targetPath))

// COMMAND ----------

// MAGIC %md Optimize the Delta data

// COMMAND ----------

// MAGIC %sql OPTIMIZE delta.`dbfs:/tmp/sreeram.nudurupati@databricks.com/delta/EDGAR-Log-20170329`

// COMMAND ----------

// MAGIC %sql DESCRIBE DETAIL delta.`dbfs:/tmp/sreeram.nudurupati@databricks.com/delta/EDGAR-Log-20170329`

// COMMAND ----------

// MAGIC %md
// MAGIC ### Data Validation
// MAGIC
// MAGIC One aspect of ETL jobs is to validate that the data is what you expect.  This includes:<br><br>
// MAGIC * Approximately the expected number of records
// MAGIC * The expected fields are present
// MAGIC * No unexpected missing values

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Take a look at the server-side errors by hour to confirm the data meets your expectations. Visualize it by selecting the bar graph icon once the table is displayed. <br><br>
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/visualization.png" style="height: 400px" style="margin-bottom: 20px; height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>

// COMMAND ----------

import org.apache.spark.sql.functions.{from_utc_timestamp, hour}

val countsDF = serverErrorDF
  .select(hour(from_utc_timestamp($"time", "GMT")).alias("hour"))
  .groupBy($"hour")
  .count()
  .orderBy($"hour")

display(countsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC The distribution of errors by hour meets the expections.  There is an uptick in errors around midnight, possibly due to server maintenance at this time.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Saving Back to DBFS
// MAGIC
// MAGIC A common and highly effective design pattern in the Databricks and Spark ecosytem involves loading structured data back to DBFS as a parquet file. Learn more about [the scalable and optimized data storage format parquet here](http://parquet.apache.org/).
// MAGIC
// MAGIC Save the parsed DataFrame back to DBFS as Delta using the `.write` method.
// MAGIC
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> All clusters have storage availiable to them in the `/tmp/` directory.  In the case of Community Edition clusters, this is a small, but helpful, amount of storage.  
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If you run out of storage, use the command `dbutils.fs.rm("/tmp/<my directory>", true)` to recursively remove all items from a directory.  Note that this is a permanent action.

// COMMAND ----------

serverErrorDF
  .write
  .mode("overwrite") // overwrites a file if it already exists
  .format("delta") //using Delta
  .save("/tmp/" + username + "/delta/log20170329/transformedLogs")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Our ETL Pipeline
// MAGIC
// MAGIC Here's what the ETL pipeline you just built looks like.  In the rest of this course you will work with more complex versions of this general pattern.
// MAGIC
// MAGIC | Code | Stage |
// MAGIC |------|-------|
// MAGIC | `val logDF = spark`                                                                       | Extract |
// MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.read`                                                           | Extract |
// MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.format("delta")`                                         | Extract |
// MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.read(<source>)`                                                  | Extract |
// MAGIC | `val serverErrorDF = logDF`                                                               | Transform |
// MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.filter(($"code" >= 500) & ($"code" < 600))`                     | Transform |
// MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.select("date", "time", "extention", "code")`                    | Transform |
// MAGIC | `serverErrorDF.write.format("delta")`                                                                     | Load |
// MAGIC | &nbsp;&nbsp;&nbsp;&nbsp;`.save(<destination>))`                                        | Load |
// MAGIC
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This is a distributed job, so it can easily scale to fit the demands of your data set.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1: Perform an ETL Job
// MAGIC
// MAGIC Write a basic ETL script that captures the 20 most active website users and load the results to DBFS.

// COMMAND ----------

display(logDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Create a DataFrame of Aggregate Statistics
// MAGIC
// MAGIC Create a DataFrame `ipCountDF` that uses `logDF` to create a count of each time a given IP address appears in the logs, with the counts sorted in descending order.  The result should have two columns: `ip` and `count`.

// COMMAND ----------

import org.apache.spark.sql.functions.desc
// TODO
val ipCountDF = // FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution
val ipCount = ipCountDF.first()
val ipCount2 = ipCount(1).asInstanceOf[Long]
val cols = ipCountDF.columns.toSet

dbTest("ET1-S-02-01-01", "213.152.28.bhe", ipCount(0))
dbTest("ET1-S-02-01-02", true, ipCount2 > 500000 && ipCount2 < 550000)
dbTest("ET1-S-02-01-03", Set("ip", "count"), cols)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 2: Save the Results
// MAGIC
// MAGIC Use your tempory folder to save the results back to DBFS as `"/tmp/" + username + "/ipCount"` in Delta format.
// MAGIC
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** If you run out of space, use `%fs rm -r /tmp/<my directory>` to recursively (and permanently) remove all items from a directory.

// COMMAND ----------

// TODO
val writePath = "/tmp/" + username + "/delta/ipCount"
// FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution
import org.apache.spark.sql.functions.desc

val ipCountDF2 = (spark
  .read
  .format("delta")
  .load(writePath)
  .orderBy(desc("count"))
)
val ipCount = ipCountDF2.first()
val ipCount2 = ipCount(1).asInstanceOf[Long]
val cols = ipCountDF2.columns.toSet

dbTest("ET1-S-02-02-01", "213.152.28.bhe", ipCount(0))
dbTest("ET1-S-02-02-02", true, ipCount2 > 500000 && ipCount2 < 550000)
dbTest("ET1-S-02-02-03", Set("ip", "count"), cols)

println("Tests passed!")

// COMMAND ----------

//Optimize ipCount table
%sql OPTIMIZE //FILL IN

// COMMAND ----------

// MAGIC %md
// MAGIC Check the load worked by using `%fs ls <path>`.  Parquet divides your data into a number of files.  If successful, you see a `_SUCCESS` file as well as the data split across a number of parts.

// COMMAND ----------

display(dbutils.fs.ls(writePath))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Review
// MAGIC **Question:** What does ETL stand for and what are the stages of the process?  
// MAGIC **Answer:** ETL stands for `extract-transform-load`
// MAGIC 0. *Extract* refers to ingesting data.  Spark easily connects to data in a number of different sources.
// MAGIC 0. *Transform* refers to applying structure, parsing fields, cleaning data, and/or computing statistics.
// MAGIC 0. *Load* refers to loading data to its final destination, usually a database or data warehouse.
// MAGIC
// MAGIC **Question:** How does the Spark approach to ETL deal with devops issues such as updating a software version?  
// MAGIC **Answer:** By decoupling storage and compute, updating your Spark version is as easy as spinning up a new cluster.  Your old code will easily connect to S3, the Azure Blob, or other storage.  This also avoids the challenge of keeping a cluster always running, such as with Hadoop clusters.
// MAGIC
// MAGIC **Question:** How does the Spark approach to data applications differ from other solutions?  
// MAGIC **Answer:** Spark offers a unified solution to use cases that would otherwise need individual tools. For instance, Spark combines machine learning, ETL, stream processing, and a number of other solutions all with one technology.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC
// MAGIC Start the next lesson, [Connecting to S3]($./03-Connecting-to-S3 ).

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC
// MAGIC **Q:** Where can I get more information on building ETL pipelines?  
// MAGIC **A:** Check out the Spark Summit talk on <a href="https://databricks.com/session/building-robust-etl-pipelines-with-apache-spark" target="_blank">Building Robust ETL Pipelines with Apache Spark</a>
// MAGIC
// MAGIC **Q:** Where can I find out more information on moving from traditional ETL pipelines towards Spark?  
// MAGIC **A:** Check out the Spark Summit talk <a href="https://databricks.com/session/get-rid-of-traditional-etl-move-to-spark" target="_blank">Get Rid of Traditional ETL, Move to Spark!</a>
// MAGIC
// MAGIC **Q:** What are the visualization options in Databricks?  
// MAGIC **A:** Databricks provides a wide variety of <a href="https://docs.databricks.com/user-guide/visualizations/index.html" target="_blank">built-in visualizations</a>.  Databricks also supports a variety of 3rd party visualization libraries, including <a href="https://d3js.org/" target="_blank">d3.js</a>, <a href="https://matplotlib.org/" target="_blank">matplotlib</a>, <a href="http://ggplot.yhathq.com/" target="_blank">ggplot</a>, and <a href="https://plot.ly/" target="_blank">plotly<a/>.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
