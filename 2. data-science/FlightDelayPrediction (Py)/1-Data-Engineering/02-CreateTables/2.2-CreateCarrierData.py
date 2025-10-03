# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC In this exercise, we will - <BR>
# MAGIC (1) Create Delta table<BR>
# MAGIC (2) Explore the source data for carriers<BR>
# MAGIC (3) Review summary stats for table and columns<BR>
# MAGIC
# MAGIC Location of source data:<BR>
# MAGIC /tmp/data/mlw/delta/carrier-master

# COMMAND ----------

# MAGIC %sql
# MAGIC USE flight_db;
# MAGIC
# MAGIC DROP TABLE IF EXISTS carrier_master;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS carrier_master
# MAGIC USING delta 
# MAGIC LOCATION '/tmp/data/mlw/delta/carrier-master';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE flight_db;
# MAGIC SHOW TABLES;

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
# MAGIC select * from flight_db.carrier_master

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4. Summary and Descriptive Statistics

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 4.1. Profile the data with table stats 

# COMMAND ----------

df = sqlContext.sql("""
select * from flight_db.carrier_master
""")

# COMMAND ----------

display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 4.2. Profile the data with specific column stats 

# COMMAND ----------

display(df.describe("carrier_cd"))
