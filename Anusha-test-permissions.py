# Databricks notebook source
# MAGIC %r
# MAGIC install.packages("caesar", repos = "https://cran.microsoft.com/snapshot/2021-07-16/")

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC sparkR.session()
# MAGIC 
# MAGIC hello <- function(x) {
# MAGIC   library(caesar)
# MAGIC   libary(lda)
# MAGIC   caesar("hello world")
# MAGIC }
# MAGIC spark.lapply(c(1, 2), hello, packages = TRUE)

# COMMAND ----------

# MAGIC %r
# MAGIC install.packages("lda", repos = "https://cran.microsoft.com/snapshot/2021-07-16/")
# MAGIC library(sparklyr)
# MAGIC sc <- spark_connect(method = 'databricks')
# MAGIC 
# MAGIC apply_caes <- function(x) {
# MAGIC   library(lda)
# MAGIC   
# MAGIC }
# MAGIC sdf_len(sc, 5) %>%
# MAGIC   spark_apply(apply_caes)

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC employee <- c('John Doe','Peter Gynn','Jolie Hope')
# MAGIC salary <- c(21000, 23400, 26800)
# MAGIC startdate <- as.Date(c('2010-11-1','2008-3-25','2007-3-14'))
# MAGIC employ <- data.frame(employee, salary, startdate)
# MAGIC employ_sdf <- as.DataFrame(employ)
# MAGIC display(employ_sdf)

# COMMAND ----------

# MAGIC %r
# MAGIC library(lda)
# MAGIC lda.collapsed.gibbs.sampler(documents, K, vocab, num.iterations, alpha,
# MAGIC eta, initial = NULL, burnin = NULL, compute.log.likelihood = FALSE,
# MAGIC trace = 0L, freeze.topics = FALSE)

# COMMAND ----------

# MAGIC %r
# MAGIC schema <- structType(structField("employee", "string"),structField("salary", "double"),structField("startdate", "date"))
# MAGIC  
# MAGIC df1 <- SparkR::dapply(employ_sdf, function(x) {
# MAGIC   library (class)
# MAGIC   library( caret )
# MAGIC   return(x)
# MAGIC }, schema)

# COMMAND ----------

# MAGIC %r
# MAGIC display(df1)

# COMMAND ----------

  # dbutils.fs.ls("/mnt/test-permissions")

# COMMAND ----------

# df2 = spark.read.format("delta").load("/mnt/test-permissions/persons1/")

# COMMAND ----------

# df2.show()

# COMMAND ----------

# dbutils.fs.ls("/mnt/test-permissions/persons1")

# COMMAND ----------

# df = spark.read.format("delta").option("path", "/mnt/test-permissions/persons")

# COMMAND ----------



# COMMAND ----------

# dbutils.fs.mounts()

# COMMAND ----------

print('hello')

# COMMAND ----------

# configs = {
#   "fs.azure.account.auth.type": "CustomAccessToken",
#   "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
# }

# # Optionally, you can add <directory-name> to the source URI of your mount point.
# dbutils.fs.mount(
#   source = "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/",
#   mount_point = "/mnt/cred-mount",
#   extra_configs = configs)

# COMMAND ----------

# dbutils.fs.ls("abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/persons")

# COMMAND ----------

# MAGIC %md Poornima login - High concurrency

# COMMAND ----------

# dbutils.fs.ls("/mnt/cred-mount")

# COMMAND ----------

# dbutils.fs.ls("/mnt/cred-mount")

# COMMAND ----------

# df2 = spark.read.format("delta").load("abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/persons")

# COMMAND ----------

# df2.show()

# COMMAND ----------

pip install xlrd==1.2.0

# COMMAND ----------

import xlrd

# COMMAND ----------

x = xlrd.xlsx

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

import openpyxl

# COMMAND ----------

import pandas
# df = pandas.read_excel(example.xlsx, engine=openpyxl)

# COMMAND ----------

pip freeze xlrd

# COMMAND ----------

# MAGIC %r
# MAGIC library(lda)

# COMMAND ----------

1+1

# COMMAND ----------


