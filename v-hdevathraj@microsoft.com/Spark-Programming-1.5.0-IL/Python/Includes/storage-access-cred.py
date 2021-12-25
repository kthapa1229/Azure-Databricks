# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://adf@hdevathrajstorage.dfs.core.windows.net/data",
  mount_point = "/mnt/adf",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %python
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC            "fs.azure.account.oauth2.client.id": "932dae52-6784-4a8f-88e1-596512f4f300",
# MAGIC            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="mysecretscope",key="mysecret"),
# MAGIC            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}
# MAGIC 
# MAGIC # Optionally, you can add <directory-name> to the source URI of your mount point.
# MAGIC dbutils.fs.mount(
# MAGIC   source = "abfss://adf@hdevathrajstorage.dfs.core.windows.net/data",
# MAGIC   mount_point = "/mnt/adf",
# MAGIC   extra_configs = configs)

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.hdevathrajstorage.dfs.core.windows.net", "B02QRvhS7QDCq5UOccWO+/NG08DOb09R7jaXEIENnysmQ7YqB14gXafV4iPP5HagNVJK95vjchICM5PTAku5xQ==")

# COMMAND ----------

dbutils.fs.unmount("/mnt/adf")

# COMMAND ----------

