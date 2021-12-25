# Databricks notebook source
pip install azure-eventhub

# COMMAND ----------


# import socket
import json
# import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://v-hdev-event.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=8Vg7sb8yR6KoVdlyXRs4rb0sbeMQ+8j3IaKxupq1nfM=", eventhub_name="v-hdev-hub")
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(EventData(get_tweets()))
        event_data_batch.add(EventData('Second event'))
        event_data_batch.add(EventData('Third event'))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

# COMMAND ----------

import socket
import sys
import requests
import requests_oauthlib
import json

# COMMAND ----------

# Replace the values below with yours
ACCESS_TOKEN = 'wbJF8DFJnM0hqQAOuRqVtsFL0'
ACCESS_SECRET = 'fSdYmUDqRBWuJ6ehBjy3YG9v1T03Dxc12IXG1HzX8Yg509I7Ma'
CONSUMER_KEY = '1194435412103696385-29nGwVojG2Br5tgOcNObsAfNyH65Az'
CONSUMER_SECRET = 'tbOpkekbcX7KhGGgfSsn2ABp8gT8cycheX0cQL0hiq94T'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

# COMMAND ----------

import requests
def get_tweets():
  url = "https://api.twitter.com/2/tweets/counts/recent?query=trump"

  payload = ""
  headers = {
  'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAAFKSMAEAAAAAp3yUndcSqud04K4aeCG3h6cP0Ow%3DURxo1weslSYa1B71NNFYSAXpcWmybWvdkd9ypPqGgspigxCNhG',
  'Cookie': 'guest_id=v1%3A162642272340386967; personalization_id="v1_qKFfH37Kg/stfhnAnNWFyQ=="'
}

  response = requests.request("GET", url, headers=headers, data=payload)
  event_data_batch = client.create_batch()
  can_add = True
  while can_add:
    try:
        event_data_batch.add(EventData(response.text))
    except ValueError:
        can_add = False  # EventDataBatch object reaches max_size.

  with client:
    client.send_batch(event_data_batch)
  return response.text

# COMMAND ----------

print(get_tweets())

# COMMAND ----------

def send_tweets_to_spark(http_resp, tcp_connection):
	for line in http_resp.iter_lines():
    	try:
        	full_tweet = json.loads(line)
        	tweet_text = full_tweet['text']
        	print("Tweet Text: " + tweet_text)
        	print ("------------------------------------------")
        	tcp_connection.send(tweet_text + '\n')
    	except:
        	e = sys.exc_info()[0]
        	print("Error: %s" % e)

# COMMAND ----------

async def main():
  client = EventHubConsumerClient.from_connection_string("Endpoint=sb://v-hdev-event.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=8Vg7sb8yR6KoVdlyXRs4rb0sbeMQ+8j3IaKxupq1nfM=", consumer_group="$Default", eventhub_name="v-hdev-hub")
  async with client:
          # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
         await client.receive(on_event=on_event,  starting_position="-1")

# COMMAND ----------

TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)

# COMMAND ----------

from azure.eventhub import EventHubConsumerClient

connection_str = 'Endpoint=sb://v-hdev-event.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=8Vg7sb8yR6KoVdlyXRs4rb0sbeMQ+8j3IaKxupq1nfM='
consumer_group = '$Default'
eventhub_name = 'v-hdev-hub'
client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=eventhub_name)
partition_ids = client.get_partition_ids()

# COMMAND ----------

print(partition_ids)

# COMMAND ----------

from azure.eventhub import EventHubProducerClient, EventData

connection_str = 'Endpoint=sb://v-hdev-event.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=8Vg7sb8yR6KoVdlyXRs4rb0sbeMQ+8j3IaKxupq1nfM='
eventhub_name = 'v-hdev-hub'
client = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)

event_data_batch = client.create_batch()
can_add = True
while can_add:
    try:
        event_data_batch.add(EventData(''))
    except ValueError:
        can_add = False  # EventDataBatch object reaches max_size.

with client:
    client.send_batch(event_data_batch)

# COMMAND ----------

import logging
# from azure import azure.eventhub
from azure.eventhub import EventHubConsumerClient

from pyspark.sql.functions import *
from pyspark.sql.types import *
ehConf = {}

connection_str = 'Endpoint=sb://v-hdev-event.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=8Vg7sb8yR6KoVdlyXRs4rb0sbeMQ+8j3IaKxupq1nfM='
ehConf['eventhubs.connectionString'] = connection_str
eventhub_name = 'v-hdev-hub'
consumer_group = '$Default'
client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=eventhub_name)
df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()
logger = logging.getLogger("azure.eventhub")
logging.basicConfig(level=logging.INFO)

# def on_event(partition_context, event):
#     logger.info("Received event from partition {}".format(partition_context.partition_id))
#     partition_context.update_checkpoint(event)

# with client:
#     client.receive(
#         on_event=on_event,
#         starting_position="-1",  # "-1" is from the beginning of the partition.
#     )
#     # receive events from specified partition:
#     # client.receive(on_event=on_event, partition_id='0')
    



# COMMAND ----------

df.head()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# query = streamingSelectDF.writeStream.format("console").start()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls('/mnt/training/')

# COMMAND ----------


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "932dae52-6784-4a8f-88e1-596512f4f300",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="mysecretscope",key="mysecret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://firstcontainer@hdevathrajstorage.dfs.core.windows.net/",
  mount_point = "/mnt/test-ac1",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/new_mount")

# COMMAND ----------

# MAGIC %sh
# MAGIC echo  "hi"> abc.txt

# COMMAND ----------

# MAGIC %sh
# MAGIC cat abc.txt

# COMMAND ----------

# MAGIC %sh
# MAGIC echo """
# MAGIC #!/bin/bash
# MAGIC set -e
# MAGIC if [[ $DB_IS_DRIVER = "TRUE" ]]; then
# MAGIC  echo "log4j.logger.shaded.databricks.v20180920_b33d810.org.apache.hadoop.fs.azurebfs=TRACE" >> /databricks/spark/dbconf/log4j/driver/log4j.properties
# MAGIC else
# MAGIC  echo "log4j.logger.shaded.databricks.v20180920_b33d810.org.apache.hadoop.fs.azurebfs=TRACE" >> /databricks/spark/dbconf/log4j/executor/log4j.properties
# MAGIC fi
# MAGIC """ > abfslogging.sh

# COMMAND ----------

cat abfslogging.sh

# COMMAND ----------

# MAGIC %sh
# MAGIC cp / //dbfs/databricks/jars

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC nslookup tdfraudsql02.ad.trustvesta.com

# COMMAND ----------

# MAGIC %sh
# MAGIC curl --request GET --url 'https://adb-3039079821613052.12.azuredatabricks.net/api/2.0/workspace-conf?keys=enableIpAccessLists' --header 'authorization: Bearer dapi4364e2b74ac16b3fd85b8aa0e94578a3-3'

# COMMAND ----------

# MAGIC %sh
# MAGIC curl -X -n 'https://adb-3039079821613052.12.azuredatabricks.net/api/2.0/workspace-conf?keys=enableIpAccessLists'

# COMMAND ----------

# MAGIC %sh
# MAGIC curl --request GET \
# MAGIC --url 'https://adb-3039079821613052.12.azuredatabricks.net/api/2.0/workspace-conf?keys=enableIpAccessLists' \
# MAGIC --header 'authorization: Bearer dapi4364e2b74ac16b3fd85b8aa0e94578a3-3'

# COMMAND ----------

# MAGIC %sh
# MAGIC curl -X PATCH -n \
# MAGIC   https://adb-3039079821613052.12.azuredatabricks.net/api/2.0/workspace-conf \
# MAGIC   --header 'authorization: Bearer dapi4364e2b74ac16b3fd85b8aa0e94578a3-3' \
# MAGIC   -d '{
# MAGIC     "enableIpAccessLists": "false"
# MAGIC     }'

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install shap

# COMMAND ----------

