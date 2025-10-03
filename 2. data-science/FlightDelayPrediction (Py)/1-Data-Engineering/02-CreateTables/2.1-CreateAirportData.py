# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC In this exercise, we will - <BR>
# MAGIC (1) Copy the source data for airports in the (transient) staging directory to the raw data directory<BR>
# MAGIC (2) Explore the source data for airports<BR>
# MAGIC (3) Review summary stats<BR>
# MAGIC
# MAGIC Location of source data:<BR>
# MAGIC /tmp/data/mlw/delta/airport-master/

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Create table using Databricks Delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC --DROP DATABASE flight_db CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS flight_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE flight_db;
# MAGIC
# MAGIC DROP TABLE IF EXISTS airport_master;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS airport_master
# MAGIC USING delta 
# MAGIC LOCATION "/tmp/data/mlw/delta/airport-master/";

# COMMAND ----------

# MAGIC %sql
# MAGIC USE flight_db;
# MAGIC SHOW tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL airport_master

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3. Explore

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 3.2. Query using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flight_db.airport_master

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4. Summary and Descriptive Statistics

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 4.1. Profile the data with table stats 

# COMMAND ----------

df = spark.sql("""
select * from flight_db.airport_master
""")

# COMMAND ----------

#Summary stats
display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 4.2. Profile the data with specific column stats 

# COMMAND ----------

display(df.describe("airport_id","airport_cd"))
