# Databricks notebook source
import pydub

# COMMAND ----------

# MAGIC %sh
# MAGIC nslookup https://pypi.org

# COMMAND ----------

dbutils.fs.head('dbfs:/mnt/new_mount996')

# COMMAND ----------

raw_data = 'resources_2020_10_01.txt'
csv_data = 'somefilename.csv'
with open(raw_data, 'r') as inp, open(csv_data, 'wb') as out:
    row = list()
    for line in inp:
        line.rstrip('\n')
        if line.startswith(','):
            row.append(line)
        else:
            out.write(''.join(row)+'\n')
            row = list()
            row.append(line)
    # Don't forget to write the last row!
    out.write(''.join(row)+'\n')

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

import psycopg2
def get_conn_cur():
  conn = psycopg2.connect(
    host = "ec2-54-174-173-31.compute-1.amazonaws.com",
    database = "d4pcv15uu5fbvp",
    user = "u7mjo8v6nha0qk",
    password = "pea1ad9161c2cd48b8a488000a5d921619c4bb698f40999565aa165c11c4cb7be",
    port = "5432")
  cur = conn.cursor()
  cur.execute("SELECT * FROM pg_database")
  result=cur.fetchone()
  return(result)
  
get_conn_cur()

# COMMAND ----------

jdbcHostname = "ec2-54-174-173-31.compute-1.amazonaws.com"
jdbcDatabase = "d4pcv15uu5fbvp"
jdbcPort = 5432
username= 'u7mjo8v6nha0qk'
password ="pea1ad9161c2cd48b8a488000a5d921619c4bb698f40999565aa165c11c4cb7be"
jdbcUrl = "jdbc:postgresql://{0}:{1}/{2}?user={3}&password={4}?sslmode=required".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)

# COMMAND ----------

jdbcUrl = "jdbc:postgresql://{0}:{1}/{2}?sslmode=require".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "org.postgresql.Driver"
}

# COMMAND ----------

pushdown_query = "(select 1 from pg_database) as alias"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# COMMAND ----------

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : "ADF_Developer",
  "password" : "dev1",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

username = "ADF_Developer"
password = "dev1"
jdbcHostname = "10.162.10.23"
jdbcDatabase = "GSEData"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}?sslmode=require".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)

# COMMAND ----------

pushdown_query = "(show tables) emp_alias"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# COMMAND ----------

# MAGIC %sh ping ACS-DWCMIALYT02.privatelink.database.windows.net

# COMMAND ----------

# MAGIC %sh telnet leavemealonesql.privatelink.database.windows.net 1433

# COMMAND ----------

# MAGIC %sh telnet leavemealonesql.database.windows.net 1433

# COMMAND ----------

import nlu as nlu

# COMMAND ----------

pip install nlu pyspark==2.4.7

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get install -y openjdk-8-jdk

# COMMAND ----------

# MAGIC %sh java -version

# COMMAND ----------

nlu.load('sentiment').predict('I love NLU! <3') 

# COMMAND ----------

# MAGIC %sh python --version

# COMMAND ----------

# MAGIC %pip list | grep nlu

# COMMAND ----------

dir(nlu)

# COMMAND ----------

help(nlu.load)

# COMMAND ----------

nlu.auth()

# COMMAND ----------

nlu.load('emotion', verbose=True).predict('wow that was easy')

# COMMAND ----------

import os
import sys

# COMMAND ----------

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# COMMAND ----------

nlu.load('sentiment', verbose=True).predict('Why is NLU so awesome? Because of the sauce!')

# COMMAND ----------

# MAGIC %sh pip list | grep spark-nlp

# COMMAND ----------

# MAGIC %sh
# MAGIC nslookup adb-4809592227509558.18.azuredatabricks.net

# COMMAND ----------

pip install azure-kusto-ingest

# COMMAND ----------

import azure-kusto-ingest

# COMMAND ----------

password = dbutils.secrets.get(scope="test-pass",key="test-pass")

# COMMAND ----------

newpass = spark.conf.get("spark.password")

# COMMAND ----------

for i in newpass:
  print(i)

# COMMAND ----------

username = "v-hdevathraj"
password = "dev1"
jdbcHostname = "10.162.10.23"
jdbcDatabase = "GSEData"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}?sslmode=require".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)

# COMMAND ----------

Hello = '123'
$Hello%123

# COMMAND ----------

123%123