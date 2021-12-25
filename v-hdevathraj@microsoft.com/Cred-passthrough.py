# Databricks notebook source
dbutils.fs.ls("/mnt/cx")

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://synapse-filesystem@pavamaadlsg2.dfs.core.windows.net/",
  mount_point = "/mnt/test-synapse-fs",
  extra_configs = configs)


# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/",
  mount_point = "/mnt/cx",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/test-synapse-fs/")
# spark.conf.set("fs.azure.account.auth.type", "CustomAccessToken")
# spark.conf.set("fs.azure.account.custom.token.provider.class",spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName"))



# COMMAND ----------

dbutils.fs.ls("abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/")

# COMMAND ----------

