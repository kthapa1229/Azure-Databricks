# Databricks notebook source
# MAGIC %md #  Model export with MLeap
# MAGIC 
# MAGIC MLeap is a common serialization format and execution engine for machine learning pipelines. It supports Apache Spark, scikit-learn, and TensorFlow for training pipelines and exporting them to an MLeap Bundle. Serialized pipelines (bundles) can be deserialized back into Apache Spark, scikit-learn, TensorFlow graphs, or an MLeap pipeline. This notebook demonstrates how to use MLeap to do the model export with MLlib. For an overview of the package and more examples, check out the [MLeap documentation](https://combust.github.io/mleap-docs/).
# MAGIC 
# MAGIC ##Requirements
# MAGIC To use MLeap, you must be running either of the following:
# MAGIC * A cluster running Databricks Runtime 6.4 or 5.5 LTS. In this case, you must install open source MLeap onto the cluster. Follow the instructions in Cmd 2. 
# MAGIC * A cluster running any version of Databricks Runtime for Machine Learning. A custom version of MLeap is included in all versions of Databricks Runtime for Machine Learning.
# MAGIC 
# MAGIC **Note:** You cannot use open source MLeap with a cluster running Databricks Runtime 7.3 LTS or above. To use MLeap with Databricks Runtime 7.3 LTS or above, you must use the corresponding version of Databricks Runtime for Machine Learning.

# COMMAND ----------

# MAGIC %md ## Install open source MLeap
# MAGIC 
# MAGIC **Note:** Skip these steps if your cluster is running Databricks Runtime for Machine Learning.
# MAGIC 
# MAGIC 1. Install MLeap-Spark.
# MAGIC 
# MAGIC    a. Create a library with the Source ``Maven Coordinate`` and the fully-qualified Maven artifact coordinate: `ml.combust.mleap:mleap-spark_2.11:0.13.0`.
# MAGIC    
# MAGIC    b. Attach the library to a cluster.
# MAGIC 
# MAGIC 1. Install MLeap.
# MAGIC 
# MAGIC    a. Create a library with the PyPI package named ``mleap``.
# MAGIC    
# MAGIC    b. Install the library in the cluster.
# MAGIC 
# MAGIC 1. Install [MLflow](https://www.mlflow.org/) so that the training calls log parameters and metrics to the MLflow tracking server.
# MAGIC 
# MAGIC    a. Create a library with the PyPI package named ``mlflow``.
# MAGIC   
# MAGIC    b. Install the library in the cluster.

# COMMAND ----------

# MAGIC %md ## In this notebook
# MAGIC 
# MAGIC This notebook demonstrates how to use MLeap to export a `DecisionTreeClassifier` from MLlib and how to load the saved PipelineModel to make predictions.
# MAGIC 
# MAGIC The basic workflow is as follows:
# MAGIC * Model export
# MAGIC   * Fit a PipelineModel using MLlib.
# MAGIC   * Use MLeap to serialize the PipelineModel to zip file or to directory.
# MAGIC * Move the PipelineModel files to your deployment project or data store.
# MAGIC * In your project
# MAGIC   * Use MLeap to deserialize the saved PipelineModel.
# MAGIC   * Make predictions.

# COMMAND ----------

# MAGIC %md ## Train the model with MLlib

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, Tokenizer, HashingTF
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# COMMAND ----------

df = spark.read.parquet("/databricks-datasets/news20.binary/data-001/training").select("text", "topic")
df.cache()
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md ### Define ML pipeline

# COMMAND ----------

labelIndexer = StringIndexer(inputCol="topic", outputCol="label", handleInvalid="keep")

# COMMAND ----------

tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="features")

# COMMAND ----------

dt = DecisionTreeClassifier()
pipeline = Pipeline(stages=[labelIndexer, tokenizer, hashingTF, dt])

# COMMAND ----------

# MAGIC %md ### Tune ML pipeline

# COMMAND ----------

paramGrid = ParamGridBuilder().addGrid(hashingTF.numFeatures, [1000, 2000]).build()
cv = CrossValidator(estimator=pipeline, evaluator=MulticlassClassificationEvaluator(), estimatorParamMaps=paramGrid)

# COMMAND ----------

cvModel = cv.fit(df)

# COMMAND ----------

model = cvModel.bestModel

# COMMAND ----------

sparkTransformed = model.transform(df)
display(sparkTransformed)

# COMMAND ----------

# MAGIC %md ## Use MLeap to export the trained model

# COMMAND ----------

# MAGIC %md
# MAGIC MLeap supports serializing the model to one zip file. In order to serialize to a zip file, make sure the URI begins with ``jar:file`` and ends with a ``.zip``.

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -rf /tmp/mleap_python_model_export
# MAGIC mkdir /tmp/mleap_python_model_export

# COMMAND ----------

import mleap.pyspark
from mleap.pyspark.spark_support import SimpleSparkSerializer

model.serializeToBundle("jar:file:/tmp/mleap_python_model_export/20news_pipeline-json.zip", sparkTransformed)

# COMMAND ----------

# MAGIC %md ## Download model files
# MAGIC In this example you download the model files from the browser. In general, you may want to programmatically move the model to a persistent storage layer.

# COMMAND ----------

dbutils.fs.cp("file:/tmp/mleap_python_model_export/20news_pipeline-json.zip", "dbfs:/example/20news_pipeline-json.zip")
display(dbutils.fs.ls("dbfs:/example"))

# COMMAND ----------

# MAGIC %md  Get a link to the downloadable zip at: `https://<databricks-instance>/files/<file-name>.zip`.

# COMMAND ----------

# MAGIC %md ## Use MLeap to import the trained model

# COMMAND ----------

# MAGIC %md 
# MAGIC This section shows how to use MLeap to load a trained model for use in your application. To use existing ML models and pipelines to make predictions for new data, you can deserialize the model from the file you saved.

# COMMAND ----------

# MAGIC %md ### Import model to PySpark
# MAGIC 
# MAGIC This section shows how to load an MLeap bundle and make predictions on a Spark DataFrame.  This can be useful if you want to use the same persistence format (bundle) for loading into Spark and non-Spark applications.  If your goal is to make predictions only in Spark, then we recommend using [MLlib's native ML persistence](https://spark.apache.org/docs/latest/ml-pipeline.html#ml-persistence-saving-and-loading-pipelines).

# COMMAND ----------

from pyspark.ml import PipelineModel
deserializedPipeline = PipelineModel.deserializeFromBundle("jar:file:/tmp/mleap_python_model_export/20news_pipeline-json.zip")

# COMMAND ----------

# MAGIC %md Use the loaded model to make predictions.

# COMMAND ----------

test_df = spark.read.parquet("/databricks-datasets/news20.binary/data-001/test").select("text", "topic")
test_df.cache()
display(test_df)

# COMMAND ----------

exampleResults = deserializedPipeline.transform(test_df)
display(exampleResults)

# COMMAND ----------

# MAGIC %md ### Import to MLeap
# MAGIC 
# MAGIC The primary use of MLeap is to import models into applications without Spark available.  These applications should be implemented in Scala or Java; see the Scala notebook matching this Python notebook for an example.