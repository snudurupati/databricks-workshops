// Databricks notebook source
// MAGIC %md
// MAGIC # Connecting to S3
// MAGIC
// MAGIC Apache Spark&trade; and Databricks&reg; allow you to connect to virtually any data store including Amazon S3.
// MAGIC ## In this lesson you:
// MAGIC * Mount and access data in S3
// MAGIC * Define options when reading from S3
// MAGIC
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
// MAGIC
// MAGIC ## Prerequisites
// MAGIC * Web browser: Please use a <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers" target="_blank">supported browser</a>.
// MAGIC * Concept (optional): <a href="https://academy.databricks.com/collections/frontpage/products/dataframes" target="_blank">DataFrames course from Databricks Academy</a>

// COMMAND ----------

// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/r2725pnugw?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/r2725pnugw?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Spark as a Connector
// MAGIC
// MAGIC Spark quickly rose to popularity as a replacement for the [Apache Hadoop&trade;](http://hadoop.apache.org/) MapReduce paradigm in a large part because it easily connected to a number of different data sources.  Most important among these data sources was the Hadoop Distributed File System (HDFS).  Now, Spark engineers connect to a wide variety of data sources including:  
// MAGIC <br>
// MAGIC * Traditional databases like Postgres, SQL Server, and MySQL
// MAGIC * Message brokers like <a href="https://kafka.apache.org/" target="_blank">Apache Kafka</a> and <a href="https://aws.amazon.com/kinesis/">Kinesis</a>
// MAGIC * Distributed databases like Cassandra and Redshift
// MAGIC * Data warehouses like Hive
// MAGIC * File types like CSV, Parquet, and Avro
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/open-source-ecosystem_2.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### DBFS Mounts and S3
// MAGIC
// MAGIC Amazon Simple Storage Service (S3) is the backbone of Databricks workflows.  S3 offers data storage that easily scales to the demands of most data applications and, by colocating data with Spark clusters, Databricks quickly reads from and writes to S3 in a distributed manner.
// MAGIC
// MAGIC The Databricks File System, or DBFS, is a layer over S3 that allows you to mount S3 buckets, making them available to other users in your workspace and persisting the data after a cluster is shut down.
// MAGIC
// MAGIC In our roadmap for ETL, this is the <b>Extract and Validate </b> step:
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ETL-Process-1.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

// COMMAND ----------

// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/wk0yb1jyz5?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/wk0yb1jyz5?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC Run the cell below to set up your environment.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC
// MAGIC Define your AWS credentials.  Below are defined read-only keys, the name of an AWS bucket, and the mount name to refer to use in DBFS.
// MAGIC
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For getting AWS keys, take a look at <a href="https://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html" target="_blank"> take a look at the AWS documentation

// COMMAND ----------

val AccessKey = "AKIAJBRYNXGHORDHZB4A"
// Encode the Secret Key to remove any "/" characters
val SecretKey = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF".replace("/", "%2F")
val AwsBucketName = "databricks-corp-training/common"
val MountName = "/mnt/training"

// COMMAND ----------

// MAGIC %md
// MAGIC Since you mounted this bucket in the classroom setup script, first you need to unmount it.

// COMMAND ----------

try {
  dbutils.fs.unmount(s"$MountName") // Use this to unmount as needed
} catch {
  case ioe: java.rmi.RemoteException => println(s"$MountName already unmounted")
}

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC
// MAGIC Now mount the bucket [using the template provided in the docs.](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html#mounting-an-s3-bucket)
// MAGIC
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The code below includes error handling logic to handle the case where the mount is already mounted.

// COMMAND ----------

try {
  dbutils.fs.mount(s"s3a://$AccessKey:$SecretKey@$AwsBucketName", s"$MountName")
} catch {
  case ioe: java.rmi.RemoteException => println($"$MountName already mounted. Run previous cells to unmount first")
}

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Next, explore the mount using `%fs ls` and the name of the mount.

// COMMAND ----------

// MAGIC %fs ls /mnt/training

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC In practice, always secure your AWS credentials.  Do this by either maintaining a single notebook with restricted permissions that holds AWS keys, or delete the cells or notebooks that expose the keys. After a cell used to mount a bucket is run, access this mount in any notebook, any cluster, and share the mount between colleagues.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Adding Options
// MAGIC
// MAGIC When you import that data into a cluster, you can add options based on the specific characteristics of the data.

// COMMAND ----------

// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/u2z99yb5p0?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/u2z99yb5p0?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC Display the first few lines of `Chicago-Crimes-2018.csv` using `%fs head`.

// COMMAND ----------

// MAGIC %fs head /mnt/training/Chicago-Crimes-2018.csv

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC `option` is a method of `DataFrameReader`. Options are key/value pairs and must be specified before calling `.csv()`.
// MAGIC
// MAGIC This is a tab-delimited file, as seen in the previous cell. Specify the `"delimiter"` option in the import statement.  
// MAGIC
// MAGIC :NOTE: Find a [full list of parameters here.](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=dateformat#pyspark.sql.DataFrameReader.csv)

// COMMAND ----------

display(spark.read
  .option("delimiter", "\t")
  .csv("/mnt/training/Chicago-Crimes-2018.csv")
)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Spark doesn't read the header by default, as demonstrated by the column names of `_c0`, `_c1`, etc. Notice that the column names are present in the first row of the DataFrame. 
// MAGIC
// MAGIC Fix this by setting the `"header"` option to `true`

// COMMAND ----------

display(spark.read
  .option("delimiter", "\t")
  .option("header", true)
  .csv("/mnt/training/Chicago-Crimes-2018.csv")
)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Spark didn't infer the schema or read the timestamp format.  Since this file uses an atypical timestamp, Spark inferred our timestamp as a string.  Change that by adding the option `"timestampFormat"` and pass it the format used in this file.  
// MAGIC
// MAGIC Set `"inferSchema"` to `true`, which triggers Spark to make an extra pass over the data to infer the schema.

// COMMAND ----------

val crimeDF = spark.read
  .option("delimiter", "\t")
  .option("header", true)
  .option("timestampFormat", "mm/dd/yyyy hh:mm:ss a")
  .option("inferSchema", true)
  .csv("/mnt/training/Chicago-Crimes-2018.csv")

display(crimeDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## The Design Pattern
// MAGIC
// MAGIC Other connections work in much the same way, whether your data sits in Cassandra, Redis, Redshift, or another common data store.  The general pattern is always:  
// MAGIC <br>
// MAGIC 1. Define the connection point
// MAGIC 2. Define connection parameters such as access credentials
// MAGIC 3. Add necessary options
// MAGIC
// MAGIC After adhering to this, read data using `spark.read.options(<option key>, <option value>).<connection_type>(<endpoint>)`.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1: Read Wikipedia Data
// MAGIC
// MAGIC Read Wikipedia data from S3, accounting for its delimiter and header.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Get a Sense for the Data
// MAGIC
// MAGIC Take a look at the head of the data, located at `/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv`.

// COMMAND ----------

// ANSWER
println(dbutils.fs.head("/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv", 100)) // this evaluates to the thing as %fs head /mnt/training/wikipedia/pageviews/pageviews_by_second.tsv

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Import the Raw Data
// MAGIC
// MAGIC Import the data **without any options** and save it to `wikiDF`. Display the result.

// COMMAND ----------

// ANSWER
val path = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

val wikiDF = spark.read.csv(path)
display(wikiDF)

// COMMAND ----------

// TEST - Run this cell to test your solution

dbTest("ET1-S-03-01-01", 7200001, wikiDF.count)
dbTest("ET1-S-03-01-02", "_c0", wikiDF.columns(0))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3: Import the Data with Options
// MAGIC
// MAGIC Import the data with options and save it to `wikiWithOptionsDF`.  Display the result.  Your import statement should account for:<br><br>  
// MAGIC
// MAGIC  - The header
// MAGIC  - The delimiter

// COMMAND ----------

// ANSWER
val path = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

val wikiWithOptionsDF = spark.read
  .option("header", true)
  .option("delimiter", "\t")
  .csv(path)

display(wikiWithOptionsDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
val cols = wikiWithOptionsDF.columns.toSet

dbTest("ET1-S-03-02-01", 7200000, wikiWithOptionsDF.count)
dbTest("ET1-S-03-02-02", Set("timestamp", "site", "requests"), cols)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Review
// MAGIC
// MAGIC **Question:** What accounts for Spark's quick rise in popularity as an ETL tool?  
// MAGIC **Answer:** Spark easily accesses data virtually anywhere it lives, and the scalable framework lowers the difficulties in building connectors to access data.  Spark offers a unified API for connecting to data making reads from a CSV file, JSON data, or a database, to provide a few examples, nearly identical.  This allows developers to focus on writing their code rather than writing connectors.
// MAGIC
// MAGIC **Question:** What is DBFS and why is it important?  
// MAGIC **Answer:** The Databricks File System (DBFS) allows access to scalable, fast, and distributed storage backed by S3 or the Azure Blob Store.
// MAGIC
// MAGIC **Question:** How do you connect your Spark cluster to S3?  
// MAGIC **Answer:** By mounting it. Mounts require AWS credentials and give access to a virtually infinite store for your data. Using AWS IAM roles provides added security since your keys will not appear in log files.  <a href="https://docs.databricks.com/user-guide/cloud-configurations/aws/iam-roles.html" target="_blank">One other option is to define your keys in a single notebook that only you have permission to access.</a> Click the arrow next to a notebook in the Workspace tab to define access permisions.
// MAGIC
// MAGIC **Question:** How do you specify parameters when reading data?  
// MAGIC **Answer:** Using `.option()` during your read allows you to pass key/value pairs specifying aspects of your read.  For instance, options for reading CSV data include `header`, `delimiter`, and `inferSchema`.
// MAGIC
// MAGIC **Question:** What is the general design pattern for connecting to your data?  
// MAGIC **Answer:** The general design pattern is as follows:
// MAGIC 0. Define the connection point.
// MAGIC 0. Define connection parameters such as access credentials.
// MAGIC 0. Add necessary options such as for headers or paralleization.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC
// MAGIC Start the next lesson, [Connecting to JDBC]($./04-Connecting-to-JDBC ).

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC
// MAGIC **Q:** Where can I find more information on DBFS?  
// MAGIC **A:** <a href="https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html" target="_blank">Take a look at the Databricks documentation for more details

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
