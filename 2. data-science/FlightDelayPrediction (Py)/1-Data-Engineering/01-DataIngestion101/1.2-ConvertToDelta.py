# Databricks notebook source
# MAGIC %md
# MAGIC # Convert CSV data to Delta format

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp/data/mlw/stagingDir/	

# COMMAND ----------

# MAGIC %md Convert Airport data

# COMMAND ----------

newNames = ["airport_id",
"airport_cd",
"airport_nm",
"airport_lat",
"airport_long"]

# COMMAND ----------

# MAGIC %fs rm -r /tmp/data/mlw/delta/airport-master/

# COMMAND ----------

(spark
 .read
  .csv("/tmp/data/mlw/stagingDir/airport-master/")
  .toDF(*newNames)
.write
  .format("delta")
  .mode("overwrite")
  .save("/tmp/data/mlw/delta/airport-master/"))

# COMMAND ----------

# MAGIC %md Convert carrier-master

# COMMAND ----------

newNames = ["carrier_indx",
"carrier_cd",
"carrier_nm" ]

# COMMAND ----------

# MAGIC %fs rm -r /tmp/data/mlw/delta/carrier-master/

# COMMAND ----------

(spark
 .read
  .csv("/tmp/data/mlw/stagingDir/carrier-master/")
  .toDF(*newNames)
.write
  .format("delta")
  .mode("overwrite")
  .save("/tmp/data/mlw/delta/carrier-master/"))

# COMMAND ----------

# MAGIC %md Convert flight-transactions

# COMMAND ----------

newNames = [
"year",
"month",
"day_of_month",
"day_of_week",
"carrier_cd",
"crs_dep_tm",
"dep_delay",
"dep_delay_15",
"crs_arr_tm",
"arr_delay",
"arr_delay_15",
"cancelled",
"origin_airport_cd",
"dest_airport_cd"]

# COMMAND ----------

# MAGIC %fs rm -r /tmp/data/mlw/delta/flight-transactions/

# COMMAND ----------

(spark
 .read
  .csv("/tmp/data/mlw/stagingDir/flight-transactions/")
  .toDF(*newNames)
.write
  .format("delta")
  .mode("overwrite")
  .save("/tmp/data/mlw/delta/flight-transactions/"))

# COMMAND ----------

# MAGIC %md Convert weather-ref

# COMMAND ----------

newNames = [
"year",
"month",
"day",
"time",
"time_zone",
"sky_condition",
"visibility",
"weather_type",
"dry_bulb_farenheit",
"dry_bulb_celsius",
"wet_bulb_farenheit",
"wet_bulb_celsius",
"dew_point_farenheit",
"dew_point_celsius",
"relative_humidity",
"wind_speed",
"wind_direction",
"value_for_wind_character",
"station_pressure",
"pressure_tendency",
"pressure_change",
"sea_level_pressure",
"record_type",
"hourly_precip",
"altimeter",
"latitude",
"longitude"]

# COMMAND ----------

# MAGIC %fs rm -r /tmp/data/mlw/delta/weather-ref/

# COMMAND ----------

(spark
 .read
  .csv("/tmp/data/mlw/stagingDir/weather-ref/")
  .toDF(*newNames)
.write
  .format("delta")
  .mode("overwrite")
  .save("/tmp/data/mlw/delta/weather-ref/"))
