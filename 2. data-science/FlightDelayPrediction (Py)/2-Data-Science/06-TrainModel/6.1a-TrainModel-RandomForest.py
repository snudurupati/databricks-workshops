# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC In this exercise, we will create a machine learning model in Spark MLLib, ML API, and persist the model for later use

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel, RandomForestClassifier

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Create dataframe of model input

# COMMAND ----------

#Source and destination
sourceDirectory = "/tmp/data/mlw/features"
destinationDirectory="/tmp/data/mlw/model/flight-delay/randomForest/manually-tuned"

# COMMAND ----------

inputDataDF = (spark
                 .read
                 .format("delta")
                 .load(sourceDirectory))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Split into training and test data

# COMMAND ----------

#Split the data into training and test sets (30% held out for testing).
(trainingDataset, testDataset) = inputDataDF.randomSplit((0.75, 0.25), seed =1234)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3. Instantiate classifier

# COMMAND ----------

randForest = (RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setNumTrees(100)
  .setMaxDepth(10)
  .setMaxBins(160))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4. Train the model with the training data split

# COMMAND ----------

#Train the model
model = randForest.fit(trainingDataset)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 5. Test the model against the test data split

# COMMAND ----------

#Make predictions on the test dataset
predictions = model.transform(testDataset)

#Select example rows to display.
display(predictions.select("prediction", "label", "probability" ,"features"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 6. Review metrics

# COMMAND ----------

#Select (prediction, true label) and compute test error.
evaluator = (MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy"))

accuracy = evaluator.evaluate(predictions)
print("Test Error = %f" % (1.0 - accuracy))
#0.794150404516767

# COMMAND ----------

#* Binary classification evaluation metrics*
evaluator = (BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("probability").setMetricName("areaUnderROC"))
ROC = evaluator.evaluate(predictions)
print("ROC on test data = %f" % (ROC))
#0.7734315360857176

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 7. Print model

# COMMAND ----------

#Print model 
print("Learned classification forest model:\n" + model.toDebugString)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 8. Persist model

# COMMAND ----------

(model
     .save(destinationDirectory))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 9. Check feature importance

# COMMAND ----------

#FEATURE IMPORTANCE
model.featureImportances



# COMMAND ----------

//0 - origin_airport_id - 0.061617093986384194
//1 - flight_month - 0.04194211627845236
//2 - flight_day_of_month - 0.11509911942769702
//3 - flight_day_of_week - 0.061531660019990865
//4 - flight_dep_hour - 0.4371281475075368
//5 - carrier_indx - 0.05770819357125712
//6 - dest_airport_id - 0.01769814715174663
//7 - wind_speed - 0.02170460496570623
//8 - sea_level_pressure - 0.12510959002658545
//9 - hourly_precip - 0.060461327064643425
