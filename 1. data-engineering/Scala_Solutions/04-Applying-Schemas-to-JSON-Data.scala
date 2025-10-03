// Databricks notebook source
// MAGIC %md
// MAGIC # Applying Schemas to JSON Data
// MAGIC
// MAGIC Apache Spark&trade; and Databricks&reg; provide a number of ways to project structure onto semi-structured data allowing for quick and easy access.
// MAGIC ## In this lesson you:
// MAGIC * Infer the schema from JSON files
// MAGIC * Create and use a user-defined schema with primitive data types
// MAGIC * Use nonprimitive data types such as `ArrayType` and `MapType` in a schema
// MAGIC
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Additional Audiences: Data Scientists and Software Engineers
// MAGIC
// MAGIC ## Prerequisites
// MAGIC * Web browser: Please use a <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers" target="_blank">supported browser</a>.
// MAGIC * Concept (optional): <a href="https://academy.databricks.com/collections/frontpage/products/dataframes" target="_blank">DataFrames course from Databricks Academy</a>

// COMMAND ----------

// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/xninybx2e2?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/xninybx2e2?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Schemas
// MAGIC
// MAGIC Schemas are at the heart of data structures in Spark.
// MAGIC **A schema describes the structure of your data by naming columns and declaring the type of data in that column.** 
// MAGIC Rigorously enforcing schemas leads to significant performance optimizations and reliability of code.
// MAGIC
// MAGIC Why is open source Spark so fast, and why is [Databricks Runtime even faster?](https://databricks.com/blog/2017/07/12/benchmarking-big-data-sql-platforms-in-the-cloud.html) While there are many reasons for these performance improvements, two key reasons are:<br><br>
// MAGIC * First and foremost, Spark runs first in memory rather than reading and writing to disk. 
// MAGIC * Second, using DataFrames allows Spark to optimize the execution of your queries because it knows what your data looks like.
// MAGIC
// MAGIC Two pillars of computer science education are data structures, the organization and storage of data and algorithms, and the computational procedures on that data.  A rigorous understanding of computer science involves both of these domains. When you apply the most relavant data structures, the algorithms that carry out the computation become significantly more eloquent.
// MAGIC
// MAGIC In the roadmap for ETL, this is the **Apply Schema** step:
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ETL-Process-2.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Schemas with Semi-Structured JSON Data
// MAGIC
// MAGIC **Tabular data**, such as that found in CSV files or relational databases, has a formal structure where each observation, or row, of the data has a value (even if it's a NULL value) for each feature, or column, in the data set.  
// MAGIC
// MAGIC **Semi-structured data** does not need to conform to a formal data model. Instead, a given feature may appear zero, once, or many times for a given observation.  
// MAGIC
// MAGIC Semi-structured data storage works well with hierarchical data and with schemas that may evolve over time.  One of the most common forms of semi-structured data is JSON data, which consists of attribute-value pairs.

// COMMAND ----------

// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/4e7wshp1ax?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/4e7wshp1ax?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC Run the following cell to mount the data:

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Print the first few lines of a JSON file holding ZIP Code data.

// COMMAND ----------

// MAGIC %fs head /mnt/training/zips.json

// COMMAND ----------

// MAGIC %md
// MAGIC ### Schema Inference
// MAGIC
// MAGIC Import data as a DataFrame and view its schema with the `printSchema()` DataFrame method.

// COMMAND ----------

val zipsDF = spark.read.json("/mnt/training/zips.json")
zipsDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Store the schema as an object by calling `.schema` on a DataFrame. Schemas consist of a `StructType`, which is a collection of `StructField`s.  Each `StructField` gives a name and a type for a given field in the data.

// COMMAND ----------

val zipsSchema = zipsDF.schema

zipsSchema.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ### User-Defined Schemas
// MAGIC
// MAGIC Spark infers schemas from the data, as detailed in the example above.  Challenges with inferred schemas include:  
// MAGIC <br>
// MAGIC * Schema inference means Spark scans all of your data, creating an extra job, which can affect performance
// MAGIC * Consider providing alternative data types (for example, change a `Long` to a `Integer`)
// MAGIC * Consider throwing out certain fields in the data, to read only the data of interest
// MAGIC
// MAGIC To define schemas, build a `StructType` composed of `StructField`s.

// COMMAND ----------

// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/jizz3og20l?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/jizz3og20l?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC Import the necessary types from the `types` module. Build a `StructType`, which takes a list of `StructField`s.  Each `StructField` takes three arguments: the name of the field, the type of data in it, and a `Boolean` for whether this field can be `Null`.

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType}

val zipsSchema2 = StructType(List(
  StructField("city", StringType, true),
  StructField("pop", IntegerType, true)
))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Apply the schema using the `.schema` method. This `read` returns only  the columns specified in the schema and changes the column `pop` from `LongType` (which was inferred above) to `IntegerType`.
// MAGIC
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> A `LongType` is an 8-byte integer ranging up to 9,223,372,036,854,775,807 while `IntegerType` is a 4-byte integer ranging up to 2,147,483,647.  Since no American city has over two billion people, `IntegerType` is sufficient.

// COMMAND ----------

val zipsDF2 = spark.read
  .schema(zipsSchema2)
  .json("/mnt/training/zips.json")

display(zipsDF2)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Primitive and Nonprimitive Types
// MAGIC
// MAGIC The Spark [`types` package](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.package) provides the building blocks for constructing schemas.
// MAGIC
// MAGIC A primitive type contains the data itself.  The most common primitive types include:
// MAGIC
// MAGIC | Numeric | General | Time |
// MAGIC |-----|-----|
// MAGIC | `FloatType` | `StringType` | `TimestampType` | 
// MAGIC | `IntegerType` | `BooleanType` | `DateType` | 
// MAGIC | `DoubleType` | `NullType` | |
// MAGIC | `LongType` | | |
// MAGIC | `ShortType` |  | |
// MAGIC
// MAGIC Non-primitive types are sometimes regarded as reference variables or composite types.  Technically, non-primitive types contain references to memory locations and not the data itself.  Nonprimitive types are the composite of a number of primitive types such as an Array of the primitive type `Integer`.
// MAGIC
// MAGIC The two most common composite types are `ArrayType` and `MapType`. These types allow for a given field to contain an arbitrary number of elements in either an Array or Map form.
// MAGIC
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the [Spark documentation](http://spark.apache.org/docs/latest/sql-programming-guide.html#data-types) for a complete picture of types in Spark.

// COMMAND ----------

// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/qk2is6llgl?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/qk2is6llgl?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC The ZIP Code dataset contains an array with the latitude and longitude of the cities.  Use an `ArrayType`, which takes the primitive type of its elements as an argument.

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType, ArrayType, FloatType}

val zipsSchema3 = StructType(List(
  StructField("city", StringType, true), 
  StructField("loc", 
    ArrayType(FloatType, true), true),
  StructField("pop", IntegerType, true)
))

// COMMAND ----------

// MAGIC %md
// MAGIC Apply the schema using the `.schema()` method and observe the results.  Expand the array values in the column `loc` to explore further.

// COMMAND ----------

val zipsDF3 = (spark.read
  .schema(zipsSchema3)
  .json("/mnt/training/zips.json")
)
display(zipsDF3)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1: Exploring JSON Data
// MAGIC
// MAGIC <a href="https://archive.ics.uci.edu/ml/datasets/UbiqLog+(smartphone+lifelogging)">Smartphone data from UCI Machine Learning Repository</a> is available under `/mnt/training/UbiqLog4UCI`. This is log data from the open source project [Ubiqlog](https://github.com/Rezar/Ubiqlog).
// MAGIC
// MAGIC Import this data and define your own schema.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Import the Data
// MAGIC
// MAGIC Import data from `/mnt/training/14_F/log*`. (This is the log files from a given user.)

// COMMAND ----------

// MAGIC %md
// MAGIC Look at the head of one file from the data set.  Use `/mnt/training/UbiqLog4UCI/14_F/log_1-6-2014.txt`.

// COMMAND ----------

// ANSWER
println(dbutils.fs.head("/mnt/training/UbiqLog4UCI/14_F/log_1-6-2014.txt", 100)) // this evaluates to the thing as %fs head /mnt/training/UbiqLog4UCI/14_F/log_1-6-2014.txt

// COMMAND ----------

// MAGIC %md
// MAGIC Read the data and save it to `smartphoneDF`. Read the logs using a `*` in your path like `/mnt/training/UbiqLog4UCI/14_F/log*`.

// COMMAND ----------

// ANSWER
val smartphoneDF = spark.read.json("/mnt/training/UbiqLog4UCI/14_F/log*")

// COMMAND ----------

// TEST - Run this cell to test your solution
import org.apache.spark.sql.functions.desc

val cols = smartphoneDF.columns.toSet
val sample = smartphoneDF.orderBy(desc("Application")).first().toSeq(0).toString

dbTest("ET1-S-05-01-01", 25372, smartphoneDF.count)
dbTest("ET1-S-05-01-02", Set("Location", "SMS", "WiFi", "_corrupt_record", "Application", "Call", "Bluetooth"), cols)
dbTest("ET1-S-05-01-03", true, sample.contains("12-9-2013 21:30:02"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Explore the Inferred Schema
// MAGIC
// MAGIC Print the schema to get a sense for the data.

// COMMAND ----------

// ANSWER
smartphoneDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC The schema shows:  
// MAGIC
// MAGIC * Six categories of tracked data 
// MAGIC * Nested data structures
// MAGIC * A field showing corrupt records

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 2: Creating a User Defined Schema

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Set Up Your Workflow
// MAGIC
// MAGIC Often the hardest part of a coding challenge is setting up a workflow to get continuous feedback on what you develop.
// MAGIC
// MAGIC Start by importing the necessary functions:
// MAGIC
// MAGIC | Package | Function |
// MAGIC |---------|---------|
// MAGIC | `org.apache.spark.sql.types` | `StructType`, `StructField`, `StringType` |

// COMMAND ----------

// ANSWER

import org.apache.spark.sql.types.{StructType, StructField, StringType}

// COMMAND ----------

// MAGIC %md
// MAGIC The **SMS** field needs to be parsed. Create a placeholder schema called `schema` that's a `StructType` with one `StructField` named **SMS** of type `StringType`. This imports the entire attribute (even though it contains nested entities) as a String.  
// MAGIC
// MAGIC This is a way to get a sense for what's in the data and make a progressively more complex schema.

// COMMAND ----------

// ANSWER

import org.apache.spark.sql.types.{StructType, StructField, StringType}

val schema = StructType(List(
  StructField("SMS", StringType, true)
))

// COMMAND ----------

// TEST - Run this cell to test your solution
val fields = schema.fields

dbTest("ET1-S-05-02-01", 1, fields.length)
dbTest("ET1-S-05-02-02", "SMS", fields(0).name)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC Apply the schema to the data and save the result as `SMSDF`. This closes the loop on which to iterate and develop an increasingly complex schema. The path to the data is `/mnt/training/UbiqLog4UCI/14_F/log*`. 
// MAGIC
// MAGIC Include only records where the column `SMS` is not `Null`.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.types.{StructType, StructField, StringType}

val schema = StructType(List(
  StructField("SMS", StringType, true)
))

val SMSDF = (spark.read
  .schema(schema)
  .json("/mnt/training/UbiqLog4UCI/14_F/log*")
  .filter($"SMS".isNotNull)
)
display(SMSDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
val cols = SMSDF.columns(0)

dbTest("ET1-S-05-03-01", 1147, SMSDF.count)
dbTest("ET1-S-05-03-02", "SMS", cols)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 2: Create the Full Schema for SMS
// MAGIC
// MAGIC Define the Schema for the following fields in the `StructType` `SMS` and name it `schema2`.  Apply it to a new DataFrame `SMSDF2`:  
// MAGIC <br>
// MAGIC * `Address`
// MAGIC * `date`
// MAGIC * `metadata`
// MAGIC  - `name`
// MAGIC  
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Note there's `Type` and `type`, which appears to be redundant data.  

// COMMAND ----------

// ANSWER

import org.apache.spark.sql.types.{StructType, StructField, StringType}

val schema2 = StructType(List(
  StructField("SMS", StructType(List(
    StructField("Address", StringType, true),
    StructField("date", StringType, true),
    StructField("metadata", StructType(List(
      StructField("name", StringType, true)
    )), true))
  ), true)
))

// Here is the full schema as well
// val fullSchema = StructType(List(
//   StructField("SMS", StructType(List(
//     StructField("Address", StringType, true),
//     StructField("Type", StringType, true),
//     StructField("body", StringType, true),
//     StructField("date", StringType, true),
//     StructField("metadata", StructType(List(
//       StructField("name", StringType, true)
//     )), true),
//     StructField("type", StringType, true)
//   )), true)
// ))

val SMSDF2 = spark.read
  .schema(schema2)
  .json("/mnt/training/UbiqLog4UCI/14_F/log*")
  .filter($"SMS".isNotNull)

display(SMSDF2)

// COMMAND ----------

// TEST - Run this cell to test your solution
val cols = SMSDF2.columns(0)
val schemaJson = SMSDF2.schema.json

dbTest("ET1-S-05-04-01", 1147, SMSDF.count)
dbTest("ET1-S-05-04-02", "SMS", cols)
dbTest("ET1-S-05-04-03", true, schemaJson.contains("Address") && schemaJson.contains("date"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3: Compare Solution Performance
// MAGIC
// MAGIC Compare the dafault schema inference to applying a user defined schema using the `%timeit` function.  Which completed faster?  Which triggered more jobs?  Why?

// COMMAND ----------

def timeIt[T](op: => T): Float = {
  val start = System.currentTimeMillis
  val res = op
  val end = System.currentTimeMillis
  (end - start) / 1000.toFloat
}

val time1 = timeIt(spark.read.schema(schema2).json("/mnt/training/UbiqLog4UCI/14_F/log*").count())
val time2 = timeIt(spark.read.json("/mnt/training/UbiqLog4UCI/14_F/log*").count())

println(s"User-defined schema completed in $time1 seconds vs $time2 seconds for schema inference")

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Providing a schema increases performance two to three times, depending on the size of the cluster used. Since Spark doesn't infer the schema, it doesn't have to read through all of the data. This is also why there are fewer jobs when a schema is provided: Spark doesn't need one job for each partition of the data to infer the schema.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Review
// MAGIC
// MAGIC **Question:** What are two ways to attain a schema from data?  
// MAGIC **Answer:** Allow Spark to infer a schema from your data or provide a user defined schema. Schema inference is the recommended first step; however, you can customize this schema to your use case with a user defined schema.
// MAGIC
// MAGIC **Question:** Why should you define your own schema?  
// MAGIC **Answer:** Benefits of user defined schemas include:
// MAGIC * Avoiding the extra scan of your data needed to infer the schema
// MAGIC * Providing alternative data types
// MAGIC * Parsing only the fields you need
// MAGIC
// MAGIC **Question:** Why is JSON a common format in big data pipelines?  
// MAGIC **Answer:** Semi-structured data works well with hierarchical data and where schemas need to evolve over time.  It also easily contains composite data types such as arrays and maps.
// MAGIC
// MAGIC **Question:** By default, how are corrupt records dealt with using `spark.read.json()`?  
// MAGIC **Answer:** They appear in a column called `_corrupt_record`.  These are the records that Spark can't read (e.g. when characters are missing from a JSON string).

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC
// MAGIC Start the next lesson, [Corrupt Record Handling]($./06-Corrupt-Record-Handling ).

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC
// MAGIC **Q:** Where can I find more information on working with JSON data?  
// MAGIC **A:** Take a look at the <a href="http://files.training.databricks.com/courses/dataframes/" target="_blank">DataFrames course from Databricks Academy</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
