# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type","OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth.provider.type","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id","a6fa3cbb-9e28-49d5-9bab-097d82dee9e8")
spark.conf.set("fs.azure.account.oauth2.client.secret",dbutils.secrets.get(scope="secret_test", key="SP"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint",f"https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.id", "a6fa3cbb-9e28-49d5-9bab-097d82dee9e8")
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.secret", dbutils.secrets.get(scope="secret_test", key="SP"))

# COMMAND ----------

spark.conf.set("fs.azure.account.key.hdevathrajstorage.dfs.core.windows.net",dbutils.secrets.get(scope="secret_test", key="storage-key"))

# COMMAND ----------

dbutils.fs.ls("abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/")

# COMMAND ----------



# COMMAND ----------

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", jdbc_url) \
  .option("user", "AbolrousHazem") \
  .option('password', "340$Uuxwp7Mcxo7Khy") \
  .option('externalDataSource', 'polybasedatascienceadls') \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("query", query) \
  .option("tempDir", temp_dir)\
  .load()

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://{host}:{port};database={database}".format(host='testing-synapse.sql.azuresynapse.net', port=1433, database='testing_pool')

temp_dir = "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/"

query = "(select top 100 * from dbo.test_error)"

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", jdbc_url) \
  .option("user", "AbolrousHazem") \
  .option('password', "340$Uuxwp7Mcxo7Khy") \
  .option("forwardSparkAzureStorageCredentials", "True") \
  .option("query", query) \
  .option("tempDir", temp_dir)\
  .load()

# COMMAND ----------

display(df.show())

# COMMAND ----------

# For appending data change mode to "append"
#Set up our connect parameters
jdbc_url = "jdbc:sqlserver://{host}:{port};database={database}".format(host='testing-synapse.sql.azuresynapse.net', port=1433, database='testing_pool')

temp_dir = "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/temp"

query = "(select top 100 * from dbo.test_error)"

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", jdbc_url) \
  .option("user", "sqladminuser") \
  .option('password', "Testing@123") \
  .option("forwardSparkAzureStorageCredentials", "True") \
  .option("query", query) \
  .option("tempDir", temp_dir)\
  .load()
df.show()


# COMMAND ----------



# COMMAND ----------

# Get some data from an Azure Synapse table using service principal working.
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://testing-synapse.sql.azuresynapse.net:1433;database=testing_pool;encrypt=true;trustServerCertificate=false;loginTimeout=30;") \
  .option("tempDir", "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/temp") \
  .option("enableServicePrincipalAuth", "True") \
  .option("forwardSparkAzureStorageCredentials", "True") \
  .option("dbTable", "dbo.employee_testqwert") \
  .load()

df.show()

# COMMAND ----------

# Get some data from an Azure Synapse table using service principal working.
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://testing-synapse.sql.azuresynapse.net:1433;database=testing_pool;encrypt=true;trustServerCertificate=false;loginTimeout=30;") \
  .option("externalDataSource", "SP_ADLS_CRED") \
  .option("tempDir", "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/temp") \
  .option("enableServicePrincipalAuth", "True") \
  .option("dbTable", "SP_ADLS_MI_TABLE") \
  .load()

df.show()

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /local_disk0/spark-57eea0b4-05cd-403e-9cff-2e870e6a39cc

# COMMAND ----------


