# Databricks notebook source
# MAGIC %python
# MAGIC  spark.conf.set(
# MAGIC    "fs.azure.account.key.hdevathrajstorage.dfs.core.windows.net",
# MAGIC    dbutils.secrets.get(scope="mysecretscope",key="accountname"))
# MAGIC spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
# MAGIC dbutils.fs.ls("abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/")
# MAGIC spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type","OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth.provider.type","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id","a6fa3cbb-9e28-49d5-9bab-097d82dee9e8")
spark.conf.set("fs.azure.account.oauth2.client.secret","mBk7Q~0YG.n9UCcJUFwOSU~3kEmjPad.U0okC")
spark.conf.set("fs.azure.account.oauth2.client.endpoint","https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.myfirstaccountdata.dfs.core.windows.net","QWjCuY6qWYXz/qmvPD/cb1PHnSF6MA05XMRo4OAcWJ5+KxHlAbMDg3lTjWrNvpwnaT4NXOZg3zziAHu4tcc1Ig==")

# COMMAND ----------

dbutils.fs.ls("abfss://newcontainerfortable@myfirstaccountdata.dfs.core.windows.net/newcontainerfortable")

# COMMAND ----------

print(secret, x)

# COMMAND ----------

dbutils.fs.ls("abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "a6fa3cbb-9e28-49d5-9bab-097d82dee9e8",
           "fs.azure.account.oauth2.client.secret": "mBk7Q~0YG.n9UCcJUFwOSU~3kEmjPad.U0okC",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/",
  mount_point = "/mnt/test-permissions",
  extra_configs = configs)

# COMMAND ----------


dbutils.fs.mount(
  source = "wasbs://firstcontainer@hdevathrajstorage.blob.core.windows.net",
  mount_point = "/mnt/sas-mount",
  extra_configs = {"fs.azure.sas.firstcontainer.hdevathraj.blob.core.windows.net":"sp=r&st=2021-12-07T20:50:26Z&se=2021-12-08T04:50:26Z&spr=https&sv=2020-08-04&sr=c&sig=qvlKSZF66VnqydyDVz9QAT8KXIlWwsWPTZpYWd1b%2FEo%3D"})

# COMMAND ----------

dbutils.fs.ls("/mnt/test-permissions")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database adf_test
# MAGIC location '/mnt/mount-adf-test'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database adf_test

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE adf_test.test2 (
# MAGIC     PersonID int,
# MAGIC     Nanmes varchar(255),
# MAGIC     FirstName varchar(255),
# MAGIC     Address varchar(255),
# MAGIC     City varchar(255)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adf_test.test2

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC drop table adf_test.test2

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO adf_test.test2 (PersonID, Nanmes, FirstName, City, Address)
# MAGIC VALUES ('123', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006');

# COMMAND ----------

# MAGIC %sql
# MAGIC select `/ast/ame` from adf_test.Persons1

# COMMAND ----------


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "932dae52-6784-4a8f-88e1-596512f4f300",
           "fs.azure.account.oauth2.client.secret": "4UB7Q~JHNQJqrADdADGbbFClNuUk4iddE7M.d",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/",
  mount_point = "/mnt/merge_csv",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set(
# MAGIC    "fs.azure.sas.destination.imageryproducts.blob.core.windows.net.Base64","?sv=2019-02-02&ss=b&srt=sco&sp=rl&se=2022-12-31T00:00:00Z&st=2020-01-01T00:00:00Z&spr=https&sig=WRTJtc5%2ByxYzqgI7Vnpi6XIPOZsxIKFo6Nth%2BTgc1Os%3D")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("/mnt/")

# COMMAND ----------

# MAGIC %python
# MAGIC  spark.conf.set(
# MAGIC    "fs.azure.account.key.leavemealonestorage.dfs.core.windows.net","nAQPXogu5qj38LzSg/WjR0lI4hcky53t71eJvc8X8sJON+PhYOY8/HFLj707354TOSMXb8WaNid9XpV1cJm7zw==")

# COMMAND ----------

# MAGIC %python
# MAGIC configs = {
# MAGIC "fs.azure.account.auth.type": "CustomAccessToken",
# MAGIC "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
# MAGIC }

# COMMAND ----------

# MAGIC %python
# MAGIC df.write.json("abfss://tad@leavemealonestorage-secondary.dfs.core.windows.net/iot_devices.json")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("abfss://tad@leavemealonestorage.dfs.core.windows.net/quotes.csv")

# COMMAND ----------

# MAGIC %python
# MAGIC df = spark.read.csv("abfss://tad@leavemealonestorage-secondary.dfs.core.windows.net/quotes.csv")
# MAGIC df.show()
# MAGIC # nAQPXogu5qj38LzSg/WjR0lI4hcky53t71eJvc8X8sJON+PhYOY8/HFLj707354TOSMXb8WaNid9XpV1cJm7zw==

# COMMAND ----------

# MAGIC %python
# MAGIC configs = {
# MAGIC   "fs.azure.account.auth.type": "CustomAccessToken",
# MAGIC   "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
# MAGIC }
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/testcsv",
# MAGIC   mount_point = "/mnt/csvmerge",
# MAGIC   extra_configs = configs)

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.unmount("/mnt/mymount")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.secrets.get('test','')

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.secrets.get(scope="mysecretscope",key="accountname")

# COMMAND ----------

# MAGIC %python
# MAGIC abc = dbutils.secrets.get(scope="mysecretscope",key="accountname")

# COMMAND ----------

# MAGIC %python
# MAGIC abc

# COMMAND ----------

# MAGIC %python
# MAGIC %pip install matplotlib

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://insights-logs-clusters@gfsdwalation.blob.core.windows.net",
# MAGIC   mount_point = "/mnt/mymount/ads",
# MAGIC   extra_configs = {"fs.azure.account.key.gfsdwalation.blob.core.windows.net":"ZHDFc/pOVBnB9MMEGjbLiMYQ/N0iP7J4Gfe+IsDlX9AT1r9G6QR1KqO52dJC1oe314ycHql3sdFq+ZjquOUNPA=="})

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("wasbs://insights-logs-clusters@gfsdwalation.blob.core.windows.net")
# MAGIC # ls /mnt/insights-logs-clusters/resourceId=/

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("/mnt/mymount/insights-logs-clusters/resourceId=")

# COMMAND ----------

# MAGIC %python
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC            "fs.azure.account.oauth2.client.id": "506d1733-1444-4fac-a4d6-59da2dd52dec",
# MAGIC            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="mysecretscope",key="mysecret"),
# MAGIC            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/51fb0dd2-ffc9-42ce-bbbc-ae375e225b96/oauth2/token"}
# MAGIC 
# MAGIC # Optionally, you can add <directory-name> to the source URI of your mount point.
# MAGIC dbutils.fs.mount(
# MAGIC   source = "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/",
# MAGIC   mount_point = "/mnt/new_mount56",
# MAGIC   extra_configs = configs)

# COMMAND ----------

# MAGIC %python
# MAGIC # spark.conf.set(
# MAGIC #   "fs.azure.account.key.gfsdwalation.blob.core.windows.net",
# MAGIC #   "ZHDFc/pOVBnB9MMEGjbLiMYQ/N0iP7J4Gfe+IsDlX9AT1r9G6QR1KqO52dJC1oe314ycHql3sdFq+ZjquOUNPA==")
# MAGIC 
# MAGIC spark.conf.set("fs.azure.account.key.gfsdwalation.dfs.core.windows.net","ZHDFc/pOVBnB9MMEGjbLiMYQ/N0iP7J4Gfe+IsDlX9AT1r9G6QR1KqO52dJC1oe314ycHql3sdFq+ZjquOUNPA==")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("abfss://insights-logs-clusters@gfsdwalation.dfs.core.windows.net/")

# COMMAND ----------

dbutils.fs.ls("abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/")

# COMMAND ----------

dbutils.fs.ls("/mnt/test-clone")

# COMMAND ----------

pip install pyod

# COMMAND ----------

dbutils.library.installPyPI('pyod')

# COMMAND ----------

# MAGIC %sh
# MAGIC openssl s_client -connect 	abcdsadlg2prod.dfs.core.windows.net:443 -tls1

# COMMAND ----------

jdbc:sqlserver://testing-synapse.sql.azuresynapse.net:1433;database=testing_pool;user=sqladminuser@testing-synapse;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url"            -> "testing-synapse.sql.azuresynapse.net:1433",
# MAGIC   "databaseName"   -> "testing_pool",
# MAGIC   "dbTable"        -> "dbo.Clients",
# MAGIC   "user"           -> "sqladminuser@testing-synapse",
# MAGIC   "password"       -> "Testing@123",
# MAGIC   "connectTimeout" -> "5", //seconds
# MAGIC   "queryTimeout"   -> "5"  //seconds
# MAGIC ))
# MAGIC 
# MAGIC val collection = spark.read.sqlDB(config)
# MAGIC collection.show()

# COMMAND ----------

spark.createDataFrame(df)

# COMMAND ----------

sql_pwd = "Testing@123"
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://testing-synapse.sql.azuresynapse.net:1433;database=testing_pool;user=sqladminuser@testing-synapse;password=" +sql_pwd + ";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;") \
  .option("tempDir", "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/temp") \
  .option("forwardSparkAzureStorageCredentials", "false") \
  .option("query", "select count(*) as cnt from test") \
  .load()

#  .option("useAzureMSI", "true")  \
# .option("enableServicePrincipalAuth", "true")

# COMMAND ----------

df.show()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

def get_current_user_name():
  return (df.withColumn("user", expr("current_user"))
          .head())

# COMMAND ----------

print(get_current_user_name())

# COMMAND ----------

df1

# COMMAND ----------

df = spark.range(10)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "932dae52-6784-4a8f-88e1-596512f4f300")
spark.conf.set("fs.azure.account.oauth2.client.secret", "4UB7Q~JHNQJqrADdADGbbFClNuUk4iddE7M.d")
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.id", "932dae52-6784-4a8f-88e1-596512f4f300")
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.secret","4UB7Q~JHNQJqrADdADGbbFClNuUk4iddE7M.d")

# COMMAND ----------



# COMMAND ----------

1+1

# COMMAND ----------

df.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC Use master
# MAGIC go
# MAGIC alter master key drop encryption by service master key

# COMMAND ----------

sql_pwd = "Testing@123"
dbtable = "test"
url="jdbc:sqlserver://testing-synapse.sql.azuresynapse.net:1433;database=testing_pool;user=sqladminuser@testing-synapse;password=" +sql_pwd + ";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
 
df.write.format("com.databricks.spark.sqldw").option("useAzureMSI", "true").mode("append").option("url", url).option("dbtable", dbtable).option("tempDir", "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/temp").save()

# COMMAND ----------

# Otherwise, set up the Blob storage account access key in the notebook session conf.
spark.conf.set(
  "fs.azure.account.key.pxpxgen2.dfs.core.windows.net","nWO9AtbHEo0alGC3TP1o9AIGskzqoQSgl8XDFoDHYmYYhaHTZ9sTT6jJhusUZDF/Avv/CrsDT+qUmN+HoPrAeQ==")
# # Load data from an Azure Synapse query.


# # Apply some transformations to the data, then use the
# # Data Source API to write the data back to another table in Azure Synapse.

# df.write \
#   .format("com.databricks.spark.sqldw") \
#   .option("url", "jdbc:sqlserver://<the-rest-of-the-connection-string>") \
#   .option("forwardSparkAzureStorageCredentials", "true") \
#   .option("dbTable", "<your-table-name>") \
#   .option("tempDir", "wasbs://<your-container-name>@<your-storage-account-name>.blob.core.windows.net/<your-directory-name>") \
#   .save()


# COMMAND ----------

dbutils.fs.ls("abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.hdevathrajstorage.dfs.core.windows.net",dbutils.secrets.get("secret_test", "storage-key"))

# COMMAND ----------

spark.conf.set("fs.azure.account.key.hdevathrajstorage.dfs.core.windows.net","B02QRvhS7QDCq5UOccWO+/NG08DOb09R7jaXEIENnysmQ7YqB14gXafV4iPP5HagNVJK95vjchICM5PTAku5xQ==")
# Get some data from an Azure Synapse table.
df = spark.read \
  .format("com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .option("url", "jdbc:sqlserver://pputtaswamysql.database.windows.net:1433;database=pputtaswamy-database;user=pputtaswamy@pputtaswamysql;password=Testing@123;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;") \
  .option("dbTable", "test_error") \
  .load()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("default.t3")

# COMMAND ----------

df['src_acct_nbr'] = "123"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from t3

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct src_acct_nbr from t2

# COMMAND ----------

jdbc:sqlserver://testing-synapse.sql.azuresynapse.net:1433;database=testing_pool;user=v-hdevathraj@microsoft.com;password=Mr-Houst305!;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;Authentication=ActiveDirectoryIntegrated

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

# MAGIC %sh
# MAGIC du -c /databricks/*

# COMMAND ----------

# MAGIC %sh
# MAGIC telnet 37.228.129.2 123

# COMMAND ----------

# MAGIC %sh ntpq -p

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type","OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type","org.apache.hadoop.fs.pxpxgen2.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth.provider.type","org.apache.hadoop.fs.pxpxgen2.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id","9f7abaac-37ba-46ff-b64b-2a9cf1b55464")
spark.conf.set("fs.azure.account.oauth2.client.secret","BCM7Q~OT3KXdUVGDaPBwzm-43ZbfkVKIJy2bv")
spark.conf.set("fs.azure.account.oauth2.client.endpoint","https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------



# COMMAND ----------

jdbcHostname = "mysql-test-vhdev.mysql.database.azure.com"
jdbcPort = "3306"
jdbcDatabase = "test_db"

# COMMAND ----------

server_name = "jdbc:mysql://mysql-test-vhdev.mysql.database.azure.com"
database_name = "mysql-test-vhdev"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "table_name"
username = "sqladminuser@mysql-test-vhdev"
password = "Testing@123" # Please specify password here
df = spark.range(10)
jdbcDF = spark.read \
        .format("org.mariadb.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password).load()
# try:
#   df.write \
#     .format("com.mysql.jdbc.Driver") \
#     .mode("overwrite") \
#     .option("url", url) \
#     .option("dbtable", table_name) \
#     .option("user", username) \
#     .option("password", password) \
#     .save()
# except ValueError as error :
#     print("Connector write failed", error)

# COMMAND ----------

jdbcDF = spark.read \
        .format("com.mysql.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password).load()

# COMMAND ----------

jdbcDF.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("org.mariadb.jdbc.Driver")

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcHostname = "mysql-test-vhdev.mysql.database.azure.com"
# MAGIC val jdbcPort = 3306
# MAGIC val jdbcDatabase = "test_db"
# MAGIC 
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?useSSL=true&requireSSL=false"
# MAGIC 
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC connectionProperties.put("user", "sqladminuser@mysql-test-vhdev")
# MAGIC connectionProperties.put("password", "Testing@123")

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.sql.DriverManager
# MAGIC val connection = DriverManager.getConnection(jdbcUrl, "sqladminuser@mysql-test-vhdev", "Testing@123")
# MAGIC connection.isClosed()

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcDF = spark.read
# MAGIC   .format("jdbc")
# MAGIC   .option("url", jdbcUrl)
# MAGIC   .option("dbtable", "test_db.test_table")
# MAGIC   .option("user", "sqladminuser@mysql-test-vhdev")
# MAGIC   .option("password", "Testing@123")
# MAGIC   .load()

# COMMAND ----------

# MAGIC %scala
# MAGIC jdbcDF.show

# COMMAND ----------

