# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "932dae52-6784-4a8f-88e1-596512f4f300",
"fs.azure.account.oauth2.client.secret": "mCzOZf_516r_-65SYCXPHxw64ZV7r_2R-6",
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}



# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
source = "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/",
mount_point = "/mnt/test-clones",
extra_configs = configs)

# COMMAND ----------

spark.conf.set("fs.azure.account.key.hdevathrajstorage.dfs.core.windows.net","B02QRvhS7QDCq5UOccWO+/NG08DOb09R7jaXEIENnysmQ7YqB14gXafV4iPP5HagNVJK95vjchICM5PTAku5xQ==")

# COMMAND ----------

data = spark.read.csv("/FileStore/shared_uploads/v-stheddu@microsoft.com/azure_log_book.csv", header="true", inferSchema="true")

# spark.sql("CREATE TABLE mysourcetable USING DELTA LOCATION '/mnt/blob-connection-test/event'")

# COMMAND ----------

data.write.format("delta").save("abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/geo-test-sruthi")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE student_test USING CSV LOCATION '/FileStore/shared_uploads/v-stheddu@microsoft.com/azure_log_book.csv'

# COMMAND ----------

spark.sql("CREATE TABLE mysourcetables USING DELTA LOCATION '/mnt/blob-connection-test/event'")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `delta`.`/mnt/test-clone/myclone` CLONE `delta`.`/mnt/test-clone/geo`
# MAGIC TBLPROPERTIES (
# MAGIC   delta.logRetentionDuration = '3650 days',
# MAGIC   delta.deletedFileRetentionDuration = '3650 days'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `delta`.`/mnt/test-clone/myclone`

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history student

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table `delta`.`dbfs:/mnt/test-clone/myclone` 
# MAGIC set TBLPROPERTIES (
# MAGIC   delta.logRetentionDuration = '30 days',
# MAGIC   delta.deletedFileRetentionDuration = '30 days'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE extended `delta`.`dbfs:/mnt/test-clone/myclone` 

# COMMAND ----------

from datetime import time
from datetime import datetime as dt
import os, uuid
from delta.tables import DeltaTable
import yaml
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import pandas as pd
from pyspark.sql.functions import col, lit, pandas_udf, regexp_extract, current_timestamp, dayofmonth, hour, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BinaryType, TimestampType, LongType, ArrayType

# COMMAND ----------

def extract_name(path_col):
  """Extract name from file path using built-in SQL functions."""
  return regexp_extract(path_col, "images/([^/]+)", 1)

# COMMAND ----------

with open('/dbfs/FileStore/configuration/prod.yml', 'r') as f:
    env = yaml.safe_load(f)

# COMMAND ----------

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
storage_container_connection_string = env['storage_container_connection_string']
storage_account = env['storage_account']
container_name = env['container_name']
queueName = env['queue_name']
tenantId = env['tenant_id']
subscriptionId = env['subscription_id']
resourceGroup = env['resource_group']
autoloaderPath = env['autoloader_path']
checkpointPath = env['checkpoint_path']
table = env['table']
mount_point = env['mount_point']
access_key = env['access_key']
event_hub_conf = {}
event_hub_connection_string = env['event_hub_connection_string']
event_hub_conf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_hub_connection_string)

# COMMAND ----------

def get_timestamp(): 
  deltaTable = DeltaTable.forName(spark, 'prod.autoloader')

  latest_history = deltaTable.history(1)
  ts_list = latest_history.select(col('timestamp'))
  ts = ts_list.first()['timestamp']
  return ts.strftime("%Y-%m-%dT%H:%M:%S.000+0000"), ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

start = get_timestamp()
start

# COMMAND ----------

eventhub_schema = ArrayType(
  StructType([
    StructField("eventTime",TimestampType(),True),
    StructField("data", StructType([
      StructField("Filename", StringType(),True),
      StructField("ImageId", StringType(), True),
      StructField("UserId", StringType(), True),
      StructField("ModemId", StringType(), True)
    ]))
  ])
)

startOffset = None # "@latest" # string containing an integer offset, "-1" for start of stream, "@latest" for end of stream
seqNo = -1
startTime = start[2] # "2021-07-07T16:01:35.000000Z" #datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

startingEventPosition = {
  "offset": startOffset,  
  "seqNo": seqNo,            #not in use
  "enqueuedTime": startTime,   #not in use
  "isInclusive": True
}

event_hub_conf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
event_hub_conf["eventhubs.maxEventsPerTrigger"] = 50000

# # # # # # # # # # # # # # # # # #
# # # # # # # TIMEOUT # # # # # # #
# # # # # # # # # # # # # # # # # #
event_hub_conf["eventhubs.receiverTimeout"] = time(0,0,30).strftime("PT%HH%MM%SS") #30 seconds
event_hub_conf["eventhubs.operationTimeout"] = time(0,0,30).strftime("PT%HH%MM%SS")

eh_df = (spark.readStream \
.format("eventhubs") \
.options(**event_hub_conf) \
.load() \
.select(col('body').cast('string').alias('body')) \
.select(from_json(col('body'), eventhub_schema).getItem(0).getItem('eventTime').alias('eventTime'), from_json(col('body'), eventhub_schema).getItem(0).getItem('data').alias('data')) \
.select(
  col('eventTime'), col('data.Filename').alias('filename'), 
  col('data.ImageId').cast('string').alias('imageId'), 
  col('data.UserId').cast('string').alias('userId'),
  col('data.ModemId').cast('string').alias('modemId')
) \
.withWatermark("eventTime", "30 seconds"))

# COMMAND ----------

# Look at storedOn
# Change storedOn to joinedOn

(spark \
.readStream \
.option("maxFilesPerTrigger", 5000) \
.option("ignoreChanges", "true") \
.option("startingTimestamp",start[1]) \
.table("prod.autoloader").withWatermark("modificationTime","30 seconds")) \
.select( "path", "content", "modificationTime", "storedOn", "day", "hour" ) \
.join(eh_df, expr("""filename=path AND eventTime >= modificationTime AND eventTime <= modificationTime + interval 2 minutes""")) \
.withColumn("joinedOn", current_timestamp()) \
.drop("filename") \
.writeStream \
.format('delta') \
.option("ignoreChanges", "true") \
.option("mergeSchema", "true") \
.option('checkpointLocation', '/ml/images_with_imageid/images/V3/checkpoints/joined/_checkpoint') \
.partitionBy("day","hour") \
.table("prod.images")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database external_database

# COMMAND ----------

import pyod

# COMMAND ----------

# MAGIC %sh
# MAGIC openssl s_client -connect http://abcdsadlg2prod.dfs.core.windows.net:443 -tls1

# COMMAND ----------

