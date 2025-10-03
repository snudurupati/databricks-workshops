// Databricks notebook source
// MAGIC %md ###Data Science Workflow
// MAGIC ![Credit Card ML](https://s3-us-west-2.amazonaws.com/databricks-aws-field-eng-root/field-eng/0/tmp/snudurupati/qbrq417/img-fraud_ml_pipeline.png)

// COMMAND ----------

// MAGIC %md
// MAGIC # Step1: Data Ingestion using Databricks Filesystem
// MAGIC 1.  Create a directory
// MAGIC 2.  Download a file to local file system
// MAGIC 3.  Save the local file to blob storage
// MAGIC 4.  List blob storage directory contents

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Download data to the driver.
// MAGIC https://flightdelayprediction.blob.core.windows.net/workshop-data/WorkshopData.zip
// MAGIC <br>Unzip the downloaded datasets

// COMMAND ----------

// MAGIC %sh wget -O /tmp/WorkshopData.zip https://flightdelayprediction.blob.core.windows.net/workshop-data/WorkshopData.zip 

// COMMAND ----------

// MAGIC %md Unzip the downloaded file

// COMMAND ----------

// MAGIC %sh unzip /tmp/WorkshopData.zip -d /tmp/

// COMMAND ----------

// MAGIC %sh ls /tmp/WorkshopData

// COMMAND ----------

// MAGIC %md ### 2. Copy a local file to Blob storage

// COMMAND ----------

// MAGIC %fs mkdirs /tmp/data/mlw/stagingDir

// COMMAND ----------

// MAGIC %fs cp -r file:/tmp/WorkshopData dbfs:/tmp/data/mlw/stagingDir

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. List the dbfs storage location

// COMMAND ----------

// MAGIC %fs ls dbfs:/tmp/data/mlw/stagingDir/	
