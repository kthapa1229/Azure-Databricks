# Databricks notebook source
# MAGIC %md-sandbox # Delta Lake - Brings Reliability and Quality to Data Lakes 
# MAGIC </p>
# MAGIC 
# MAGIC <div><img src="https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" style="height: 350px"/></div>
# MAGIC 
# MAGIC 
# MAGIC ### For more information:
# MAGIC https://delta.io/
# MAGIC * The core abstraction of the Delta Lake is an ACID compliant Spark Table.
# MAGIC * Integrate BATCH and STREAMING data
# MAGIC * Schema Enforcement and Schema Evolution
# MAGIC * ACID transactions - Multiple writers can simultaneously modify a data set and see consistent views.
# MAGIC * TIME TRAVEL - Look back on how data looked like in the past

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Scenario
# MAGIC 
# MAGIC You are a Data Engineer working for a company that processes data collected from many IoT devices.  You've been tasked to build an end-to-end pipeline to capture and process this data in near real-time (NRT).  You can think of a few creative ways to accomplish this using Apache Spark and open formats such as Parquet, but in the design process you've uncovered some challenges:
# MAGIC 
# MAGIC 
# MAGIC * Many small files leading to bad downstream performance
# MAGIC * Frequent changes to business logic, therefore data schema
# MAGIC 
# MAGIC The frequent changes to business logic is the scariest, as we may need to backfill the data multiple times.
# MAGIC 
# MAGIC We need a reliable way to update the old data as we are streaming the latest data. Having a pipeline that accommodates maximum flexibility would make our life much easier. Delta Lake's ACID guarantees and unified batch/streaming support make it a good fit.

# COMMAND ----------

# MAGIC %md ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Sample Dataset

# COMMAND ----------

# MAGIC %md ##### Setup and initialize variables

# COMMAND ----------

PARQUET_PATH="/tmp01/delta_tutorial/parquet_table"
DELTA_SILVER_PATH="/tmp01/delta_tutorial/delta_table"
DELTA_GOLD_PATH="/tmp01/delta_tutorial/delta_agg_table"

# Reset Env
dbutils.fs.rm(PARQUET_PATH, True)
dbutils.fs.rm(DELTA_SILVER_PATH, True)
dbutils.fs.rm(DELTA_GOLD_PATH, True)

# Make some configurations small-scale friendly
sql("set spark.sql.shuffle.partitions = 1")
sql("set spark.databricks.delta.snapshotPartitions = 1")

# COMMAND ----------

# MAGIC %md ##### Create a dataframe to read in our sample dataset

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.types import *

raw_data = spark.range(100000) \
  .selectExpr("if(id % 2 = 0, 'Open', 'Close') as action") \
  .withColumn("date", expr("cast(concat('2019-04-', cast(rand(5) * 30 as int) + 1) as date)")) \
  .withColumn("device_id", expr("cast(rand(5) * 100 as int)"))

# COMMAND ----------

raw_data.groupBy("action").count().show()

# COMMAND ----------

display(raw_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Using Parquet

# COMMAND ----------

# MAGIC %md #####Write this out to parquet

# COMMAND ----------

raw_data.write.format("parquet").partitionBy("date").save(PARQUET_PATH)

# COMMAND ----------

display(spark.read.format("parquet").load(PARQUET_PATH))

# COMMAND ----------

display(spark.read.format("parquet").load(PARQUET_PATH).groupBy("action").count())

# COMMAND ----------

# MAGIC %md #####Append some data to our table

# COMMAND ----------

stream_data = spark.readStream.format("rate").option("rowsPerSecond", 100).load() \
  .selectExpr("'Open' as action") \
  .withColumn("date", expr("cast(concat('2019-04-', cast(rand(5) * 30 as int) + 1) as date)")) \
  .withColumn("device_id", expr("cast(rand(5) * 500 as int)"))

# COMMAND ----------

stream_data.writeStream.format("parquet").partitionBy("date").outputMode("append") \
  .trigger(processingTime='5 seconds').option('checkpointLocation', PARQUET_PATH + "/_chk").start(PARQUET_PATH)

# COMMAND ----------

display(spark.read.format("parquet").load(PARQUET_PATH).groupBy("action").count())

# COMMAND ----------

# MAGIC %md Uh-oh. What happened to our Close actions?
# MAGIC 
# MAGIC  - You can't combine streaming and batch in the FileSink implementation in Apache Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Using Delta Lake

# COMMAND ----------

# MAGIC %md #####The syntax to create this table is almost identical, except we're using the format DELTA instead of PARQUET.

# COMMAND ----------

raw_data.write.format("delta").partitionBy("date").save(DELTA_SILVER_PATH)

# COMMAND ----------

display(spark.read.format("delta").load(DELTA_SILVER_PATH).groupBy("action").count())

# COMMAND ----------

stream_data.writeStream.format("delta").partitionBy("date").outputMode("append") \
  .trigger(processingTime='5 seconds').option('checkpointLocation', DELTA_SILVER_PATH + "/_chk").start(DELTA_SILVER_PATH)

# COMMAND ----------

display(spark.read.format("delta").load(DELTA_SILVER_PATH).groupBy("action").count())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Using our Delta Lake as a Source

# COMMAND ----------

delta_data_stream = spark.readStream \
  .option("maxFilesPerTrigger", "10") \
  .format("delta") \
  .load(DELTA_SILVER_PATH)
  

# COMMAND ----------

delta_data_stream.groupBy("action", "date", "device_id") \
  .count() \
  .writeStream \
  .format("delta") \
  .option("checkpointLocation", DELTA_GOLD_PATH + "/_checkpoint") \
  .partitionBy("date") \
  .outputMode("complete") \
  .start(DELTA_GOLD_PATH)

# COMMAND ----------

display(spark.read.format("delta").load(DELTA_GOLD_PATH).orderBy("date", "device_id", "action"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Schema Evolution

# COMMAND ----------

# Now we have more users, so let's add the user_id column to our table
new_data_with_new_col = spark.range(1000) \
  .selectExpr("'Open' as action","cast(concat('2019-04-', cast(rand(5) * 3 as int) + 1) as date) as date") \
  .withColumn("device_id", expr("cast(rand(5) * 100 as int)")) \
  .withColumn("user_id", expr("cast(rand(10) * 100 as int)"))

# COMMAND ----------

new_data_with_new_col.write.format("delta").partitionBy("date").mode("append").save(DELTA_SILVER_PATH)

# COMMAND ----------

# Add the mergeSchema option
new_data_with_new_col.write.option("mergeSchema","true").format("delta").partitionBy("date").mode("append").save(DELTA_SILVER_PATH)

# COMMAND ----------

display(spark.read.format("delta").load(DELTA_SILVER_PATH))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) ACID Transactions

# COMMAND ----------

# MAGIC %md Let's go and take a look what happened to our streams

# COMMAND ----------

# Let's add the user_id column, changing some old partitions at a time
spark.read.format("delta").load(DELTA_SILVER_PATH) \
  .where("date = '2019-05-01'") \
  .withColumn("user_id", expr("coalesce(user_id, cast(rand(10) * 100 as int))")) \
  .repartition(30, "date") \
  .write.format("delta").mode("overwrite") \
  .option("replaceWhere", "date = '2019-05-01'") \
  .save(DELTA_SILVER_PATH)

# COMMAND ----------

# MAGIC %md Delta doesn't support Hive style dynamic partition overwrites. It is bad practice, and leads to mistakes. Delta provides 'replaceWhere' to enforce data correctness.

# COMMAND ----------

# Let's restart the stream with the addition of user_id
spark.readStream \
  .option("maxFilesPerTrigger", "10") \
  .format("delta") \
  .load(DELTA_SILVER_PATH) \
  .groupBy("action", "date", "device_id", "user_id") \
  .count() \
  .writeStream \
  .format("delta") \
  .option("checkpointLocation", DELTA_GOLD_PATH + "/_checkpoint_new") \
  .option("overwriteSchema", "true") \
  .partitionBy("date") \
  .outputMode("complete") \
  .start(DELTA_GOLD_PATH)

# COMMAND ----------

display(spark.read.format("delta").load(DELTA_GOLD_PATH).orderBy("date", "device_id", "action"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Time Travel

# COMMAND ----------

# MAGIC %md The transaction log is stored along with the data under the `_delta_log` directory.

# COMMAND ----------

print("\n".join([f.name for f in dbutils.fs.ls(DELTA_SILVER_PATH + "/_delta_log") if f.name.endswith('json')]))

# COMMAND ----------

# MAGIC %fs ls /tmp01/delta_tutorial/delta_agg_table/_delta_log/

# COMMAND ----------

# latest version - 2, because in latest version - 1 we added the user_id column
version_before_schema_change = 5

# COMMAND ----------

display(spark.read.format("delta").load(DELTA_SILVER_PATH))

# COMMAND ----------

spark.read.format("delta").load(DELTA_SILVER_PATH).count()

# COMMAND ----------

display(spark.read.format("delta").option("versionAsOf", "7").load(DELTA_SILVER_PATH))

# COMMAND ----------

spark.read.format("delta").option("versionAsOf", "5").load(DELTA_SILVER_PATH).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Additional Topics & Resources
# MAGIC * <a href="https://docs.delta.io/latest/index.html" target="_blank">Delta Lake Documentation</a>
# MAGIC * <a href="https://delta.io" target="_blank">Delta Lake Website</a>