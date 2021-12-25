# Databricks notebook source
import tensorflow as tf
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential
import mlflow
import mlflow.keras
import mlflow.tensorflow

# COMMAND ----------

keras_model = mlflow.keras.load_model(f"models:/cal_housing_keras/2")

# COMMAND ----------

pip install deltaTools

# COMMAND ----------

dbutils.notebook.exit(6)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC python

# COMMAND ----------

