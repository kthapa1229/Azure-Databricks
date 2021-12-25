# Databricks notebook source
# snowflake connection options
options = {
  "sfUrl": "jo64979.west-us-2.azure.snowflakecomputing.com",
  "sfUser": "hdevathraj",
  "sfPassword": "Qwerty@1",
  "sfSchema" :"TPCH_SF1000",
  "sfDatabase": "SNOWFLAKE-SAMPLE-DATA",
  "sfWarehouse": "COMPUTE_WH"
}

# COMMAND ----------



# COMMAND ----------

# Generate a simple dataset containing five values and write the dataset to Snowflake.
# spark.range(5).write \
#   .format("snowflake") \
#   .options(**options) \
#   .option("dbtable", "<snowflake-database>") \
#   .save()

# COMMAND ----------

store_sales = """select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."STORE_SALES" """

# COMMAND ----------

item = """select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."ITEM" """

# COMMAND ----------

Customers = """select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."CUSTOMER" """

# COMMAND ----------

Web_sales = """select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."WEB_SALES"  """

# COMMAND ----------

inventory = """select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."INVENTORY" """

# COMMAND ----------

store_sales_data = spark.read \
  .format("net.snowflake.spark.snowflake") \
  .options(**options) \
  .option("query",  store_sales) \
  .load()
display(store_sales_data)

# COMMAND ----------



# COMMAND ----------

item_data = spark.read \
  .format("net.snowflake.spark.snowflake") \
  .options(**options) \
  .option("query",  item) \
  .load()
display(item_data)

# COMMAND ----------

Web_sales_data = spark.read \
  .format("net.snowflake.spark.snowflake") \
  .options(**options) \
  .option("query",  Web_sales) \
  .load()
display(Web_sales_data)
Web_sales_data.createOrReplaceTempView("web_sales_data")

# COMMAND ----------

result = spark.sql(""" select WS_BILL_CUSTOMER_SK, WS_ITEM_SK, WS_BILL_ADDR_SK from web_sales_data where WS_BILL_CUSTOMER_SK = "3414134" """).show()

# COMMAND ----------

websales_cust = (Web_sales_data.select("WS_BILL_CUSTOMER_SK", "WS_ITEM_SK", "WS_SOLD_DATE_SK")
  .where("WS_BILL_CUSTOMER_SK = 3414134").show(72))

# COMMAND ----------

websales_cust.count()

# COMMAND ----------

Web_sales_data.count()

# COMMAND ----------

Customers_data = spark.read \
  .format("net.snowflake.spark.snowflake") \
  .options(**options) \
  .option("query",  Customers) \
  .load()


# COMMAND ----------

display(Customers_data)

# COMMAND ----------

(Customers_data.select("C_CUSTOMER_SK", "C_FIRST_NAME", "C_LAST_NAME")
  .where("C_CUSTOMER_SK = 3414134")
  .orderBy("C_CUSTOMER_SK", ascending=False).show(10))

# where C_CUSTOMER_SK = 30269473

# COMMAND ----------

Customers_data.count()

# COMMAND ----------

inventory_data = spark.read \
  .format("net.snowflake.spark.snowflake") \
  .options(**options) \
  .option("query",  inventory) \
  .load()
display(inventory_data)

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.text('Query')

# COMMAND ----------


