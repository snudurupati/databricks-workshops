# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC In this exercise, we will select the required label and features we have identified for model training and create a dataset in LIBSVM format
# MAGIC
# MAGIC The label is dep_delay_15 and the features of interest are -<BR>
# MAGIC   origin_airport_id, flight_month, flight_day_of_month,flight_day_of_week, flight_dep_hour, carrier_indx, dest_airport_id, wind_speed,sea_level_pressure,hourly_precip
# MAGIC

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 1. Select attributes of interest

# COMMAND ----------

# MAGIC %sql
# MAGIC --Query to capture model training input
# MAGIC --All input needs to be numeric, and specifically double datatype
# MAGIC select distinct dep_delay_15,origin_airport_id,flight_month,flight_day_of_month,flight_day_of_week,flight_dep_hour,carrier_indx,dest_airport_id,wind_speed,sea_level_pressure,hourly_precip from flight_db.materialized_view

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 2.  Create dataframe of model input

# COMMAND ----------

#Create dataframe from query developed above
#Drop rows with any null values
modelInputDF = sqlContext.sql("""
select distinct 
cast(dep_delay_15 as double),
cast(origin_airport_id as double),
cast(flight_month as double),
cast(flight_day_of_month as double),
cast(flight_day_of_week as double),
cast(flight_dep_hour as double),
cast(carrier_indx as double),
cast(dest_airport_id as double),
cast(wind_speed as double),
cast(sea_level_pressure as double),
cast(hourly_precip as double) 
from flight_db.materialized_view
""").na.drop()

# COMMAND ----------

#Record count
modelInputDF.count()

# COMMAND ----------

display(modelInputDF)

# COMMAND ----------

categoricalCols = ["origin_airport_id", "carrier_indx", "dest_airport_id"]
continuousCols = [ "flight_month", "flight_day_of_month","flight_day_of_week", "flight_dep_hour", "wind_speed","sea_level_pressure","hourly_precip"]
labelCol = 'dep_delay_15'
df = modelInputDF

# COMMAND ----------

indexers = [ StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c))
                 for c in categoricalCols ]
# default setting: dropLast=True
encoders = [ OneHotEncoder(inputCol=indexer.getOutputCol(),
                 outputCol="{0}_encoded".format(indexer.getOutputCol()))
                 for indexer in indexers ]

assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders]
                                + continuousCols, outputCol="features")

pipeline = Pipeline(stages=indexers + encoders + [assembler])
model=pipeline.fit(df)
data = model.transform(df)
data = data.withColumn('label',col(labelCol))
vectorizedDF = data.select('features','label')

# COMMAND ----------

display(vectorizedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 5.  Persist the feature vector as a Delta table

# COMMAND ----------

destinationDirectory="/tmp/data/mlw/features"
dbutils.fs.rm(destinationDirectory, True)

# COMMAND ----------

(vectorizedDF
         .write
         .format("delta")
         .save(destinationDirectory))

# COMMAND ----------

# MAGIC %fs ls /tmp/data/mlw/features
