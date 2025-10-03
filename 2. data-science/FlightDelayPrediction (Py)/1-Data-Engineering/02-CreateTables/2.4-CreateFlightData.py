# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC In this exercise, we will - <BR>
# MAGIC (1) Create Delta table<BR>
# MAGIC (2) Explore the source data for flights<BR>
# MAGIC (3) Review summary stats<BR>
# MAGIC (4) Visualize<BR>
# MAGIC
# MAGIC Location of source data:</BR>
# MAGIC /mnt/data/mlw/stagingDir/flight-transactionslightDelaysWithAirportCodes-part[1-3].csv

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Create Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE flight_db;
# MAGIC
# MAGIC DROP TABLE IF EXISTS flight_history;
# MAGIC CREATE TABLE IF NOT EXISTS flight_history
# MAGIC USING delta
# MAGIC LOCATION '/tmp/data/mlw/delta/flight-transactions/';

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
# MAGIC select * from flight_db.flight_history

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
select * from flight_db.flight_history
""")

# COMMAND ----------

#Summary stats
display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 4.2. Profile the data with specific column stats 

# COMMAND ----------

display(df.describe("dep_delay_15"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 5. Visualization

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC SELECT carrier_cd,count(dep_delay_15) as delay_count FROM 
# MAGIC flight_db.flight_history where dep_delay_15=1
# MAGIC group by carrier_cd
# MAGIC order by delay_count desc) x limit 10

# COMMAND ----------

display(sqlContext.sql("select case dep_delay_15 when 1 then 'delayed' else 'on-time' end delayed, count(dep_delay_15) from flight_db.flight_history where dep_delay_15 is not null group by dep_delay_15"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select month,count(dep_delay_15) as delayed_count from flight_db.flight_history where dep_delay_15=1 group by month

# COMMAND ----------

# MAGIC %sql 
# MAGIC select month,day_of_week,count(dep_delay_15) as delayed_count from flight_db.flight_history where dep_delay_15=1 group by month,day_of_week order by month,day_of_week

# COMMAND ----------

# MAGIC %sql 
# MAGIC select month,day_of_month,count(dep_delay_15) as delayed_count from flight_db.flight_history where dep_delay_15=1 group by month,day_of_month order by month,day_of_month
