# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC In this exercise, we will join all 4 datasets - flight history curated, weather history curated, carrier master and airport master data to create a denormalized dataset for our modeling exercise

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 0. Before we go ahead and create the materialized views, since our data is already in Parquet format.
# MAGIC It would be beneficial to enable DBIO cache so any repeated operations on the same datasets are cached in memory and local disks.

# COMMAND ----------

#Enable DBIO cache at the cluster level
spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "1g")
spark.conf.set("spark.databricks.io.cache.compression.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Create materialized view

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 1.1. Explore

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use flight_db;
# MAGIC
# MAGIC select distinct  
# MAGIC fhc.dep_delay_15,
# MAGIC amo.airport_id as origin_airport_id,
# MAGIC fhc.origin_airport_cd,
# MAGIC amo.airport_nm as origin_airport_nm,
# MAGIC amo.airport_lat as origin_airport_latitude,
# MAGIC amo.airport_long as origin_airport_longitude,
# MAGIC fhc.month as flight_month,
# MAGIC fhc.day_of_month as flight_day_of_month,
# MAGIC fhc.day_of_week as flight_day_of_week,
# MAGIC fhc.dep_hour as flight_dep_hour, 
# MAGIC cm.carrier_indx,
# MAGIC fhc.carrier_cd,
# MAGIC cm.carrier_nm,
# MAGIC amd.airport_id as dest_airport_id,
# MAGIC fhc.dest_airport_cd,
# MAGIC amd.airport_nm as dest_airport_nm,
# MAGIC amd.airport_lat as dest_airport_latitude,
# MAGIC amd.airport_long as dest_airport_longitude,
# MAGIC whc.wind_speed,
# MAGIC whc.sea_level_pressure,
# MAGIC whc.hourly_precip
# MAGIC from 
# MAGIC flight_history_curated fhc
# MAGIC left outer join 
# MAGIC carrier_master cm
# MAGIC on (fhc.carrier_cd = cm.carrier_cd)
# MAGIC left outer join
# MAGIC airport_master amo
# MAGIC on (fhc.origin_airport_cd=amo.airport_cd)
# MAGIC left outer join
# MAGIC airport_master amd
# MAGIC on (fhc.dest_airport_cd=amd.airport_cd)
# MAGIC left outer join
# MAGIC weather_history_curated whc
# MAGIC on (whc.latitude=amo.airport_lat AND
# MAGIC     whc.longitude=amo.airport_long AND
# MAGIC     fhc.day_of_month=whc.day AND
# MAGIC     fhc.month=whc.month AND
# MAGIC     fhc.dep_hour=whc.hour)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 1.2. Create

# COMMAND ----------

matViewDF = sqlContext.sql("""
select distinct  
fhc.dep_delay_15,
amo.airport_id as origin_airport_id,
fhc.origin_airport_cd,
amo.airport_nm as origin_airport_nm,
amo.airport_lat as origin_airport_latitude,
amo.airport_long as origin_airport_longitude,
fhc.month as flight_month,
fhc.day_of_month as flight_day_of_month,
fhc.day_of_week as flight_day_of_week,
fhc.dep_hour as flight_dep_hour, 
cm.carrier_indx,
fhc.carrier_cd,
cm.carrier_nm,
amd.airport_id as dest_airport_id,
fhc.dest_airport_cd,
amd.airport_nm as dest_airport_nm,
amd.airport_lat as dest_airport_latitude,
amd.airport_long as dest_airport_longitude,
whc.wind_speed,
whc.sea_level_pressure,
whc.hourly_precip
from 
flight_history_curated fhc
left outer join 
carrier_master cm
on (fhc.carrier_cd = cm.carrier_cd)
left outer join
airport_master amo
on (fhc.origin_airport_cd=amo.airport_cd)
left outer join
airport_master amd
on (fhc.dest_airport_cd=amd.airport_cd)
left outer join
weather_history_curated whc
on (whc.latitude=amo.airport_lat AND
    whc.longitude=amo.airport_long AND
    fhc.day_of_month=whc.day AND
    fhc.month=whc.month AND
    fhc.dep_hour=whc.hour)
""")

# COMMAND ----------

matViewDF.count()

# COMMAND ----------

matViewDF.printSchema()

# COMMAND ----------

[print(x) for x in matViewDF.take(2)]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 1.3. Persist

# COMMAND ----------

destinationDirectory="/tmp/data/mlw/consumptionDir/matView"
dbutils.fs.rm(destinationDirectory, True)

# COMMAND ----------

#Persist as Delta 
(matViewDF
      .write
      .mode("overwrite")
      .format("delta")
      .save(destinationDirectory))

# COMMAND ----------

#Check destination directory
display(dbutils.fs.ls(destinationDirectory))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Create external table using Databricks Delta

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 2.1. Create table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE flight_db;
# MAGIC
# MAGIC DROP TABLE IF EXISTS materialized_view;
# MAGIC CREATE TABLE IF NOT EXISTS materialized_view
# MAGIC USING delta
# MAGIC LOCATION '/tmp/data/mlw/consumptionDir/matView';

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE flight_db;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 2.4. Query the table

# COMMAND ----------

# MAGIC %sql
# MAGIC use flight_db;
# MAGIC select * from materialized_view 
