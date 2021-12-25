# Databricks notebook source

module_name = "spark-programming"
spark.conf.set("com.databricks.training.module-name", module_name)
spark.conf.set(
    "fs.azure.account.key.hdevathrajstorage.dfs.core.windows.net", "B02QRvhS7QDCq5UOccWO+/NG08DOb09R7jaXEIENnysmQ7YqB14gXafV4iPP5HagNVJK95vjchICM5PTAku5xQ==")

# salesPath = "/mnt/training/ecommerce/sales/sales.parquet"
# usersPath = "/mnt/training/ecommerce/users/users.parquet"
# eventsPath = "/mnt/training/ecommerce/events/events.parquet"
# productsPath = "/mnt/training/ecommerce/products/products.parquet"

# COMMAND ----------

# MAGIC %run ./Common-Notebooks/Common