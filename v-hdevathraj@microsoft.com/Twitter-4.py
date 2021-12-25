# Databricks notebook source
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json
import asyncio
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
        event_data_batch.add(EventData('First event '))
        event_data_batch.add(EventData('Second event'))
        event_data_batch.add(EventData('Third event'))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())





consumer_key='LovGCp4taXgOGclj9Wum2XEda'
consumer_secret='d2NhvrqiEhsp8iBBZ1zsN5fl79AOwr0DOktoNqzIC56bK5NCLG'
access_token ='1323237425498435584-9TGePmcG2nq56MmN71K5jh7TBWd1AX'
access_secret='nhLAqoCicm4FJXwRw75MeQTeXxRu9L0Us4RqVxXgQP9AQ'

class TweetsListener(StreamListener):
  # tweet object listens for the tweets
  def __init__(self, csocket):
    self.client_socket = csocket
  def on_data(self, data):
    try:  
      msg = json.loads( data )
      print("new message")
      # if tweet is longer than 140 characters
      if "extended_tweet" in msg:
        # add at the end of each tweet "t_end" 
        self.client_socket\
            .send(str(msg['extended_tweet']['full_text']+"t_end")\
            .encode('utf-8'))         
        print(msg['extended_tweet']['full_text'])
      else:
        # add at the end of each tweet "t_end" 
        self.client_socket\
            .send(str(msg['text']+"t_end")\
            .encode('utf-8'))
        print(msg['text'])
      return True
    except BaseException as e:
        print("Error on_data: %s" % str(e))
    return True
  def on_error(self, status):
    print(status)
    return True

def sendData(c_socket, keyword):
  print('start sending data from Twitter to socket')
  # authentication based on the credentials
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)
  # start sending data from the Streaming API 
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track = keyword, languages=["en"])

if __name__ == "__main__":
    # server (local machine) creates listening socket
    s = socket.socket()
    host = "0.0.0.0"    
    port = 5555
#     s.bind((host, port))
#     print('socket is ready')
#     # server (local machine) listens for connections
#     s.listen(4)
#     print('socket is listening')
#     # return the socket and the address on the other side of the connection (client side)
#     c_socket, addr = s.accept()
#     print("Received request from: " + str(addr))
#     # select here the keyword for the tweet data
#     sendData(c_socket, keyword = ['piano'])
    async def run():
        # Create a producer client to send messages to the event hub.
        # Specify a connection string to your event hubs namespace and
        # the event hub name.
        producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://v-hdev-event.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=8Vg7sb8yR6KoVdlyXRs4rb0sbeMQ+8j3IaKxupq1nfM=", eventhub_name="v-hdev-hub")
        async with producer:
            # Create a batch.
            event_data_batch = await producer.create_batch()

            # Add events to the batch.
            event_data_batch.add(EventData(sendData('kohli')))
            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())

# COMMAND ----------

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://v-hdev-event.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=8Vg7sb8yR6KoVdlyXRs4rb0sbeMQ+8j3IaKxupq1nfM=", eventhub_name="v-hdev-hub")
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(EventData('First event '))
        event_data_batch.add(EventData('Second event'))
        event_data_batch.add(EventData('Third event'))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())

# COMMAND ----------

import http.client

conn = http.client.HTTPSConnection("api.twitter.com")
payload = ''
headers = {
  'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAAFKSMAEAAAAAp3yUndcSqud04K4aeCG3h6cP0Ow%3DURxo1weslSYa1B71NNFYSAXpcWmybWvdkd9ypPqGgspigxCNhG',
  'Cookie': 'guest_id=v1%3A162642272340386967; personalization_id="v1_qKFfH37Kg/stfhnAnNWFyQ=="'
}
conn.request("GET", "/2/tweets/sample/stream", payload, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))

# COMMAND ----------

