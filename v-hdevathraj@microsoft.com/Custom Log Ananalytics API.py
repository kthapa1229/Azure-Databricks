# Databricks notebook source
#Python script on how to submit custom data logs with the Azure Monitor HTTP Data Collector API.
# https://faun.pub/write-custom-logs-on-log-analytics-through-databricks-on-azure-68759dbeac55


import json
import requests
import datetime
import hashlib
import hmac
import base64

#Retrieve your Log Analytics Workspace ID from your Key Vault Databricks Secret Scope
#wks_id = dbutils.secrets.get(scope = "keyvault_scope", key = "wks-id-logaw1")

wks_id = "c3bc616f-XXXX-4a02-9596-29789f77a748"

#Retrieve your Log Analytics Primary Key from your Key Vault Databricks Secret Scope
#wks_shared_key = dbutils.secrets.get(scope = "keyvault_scope", key = "wks-shared-key-logaw1")
wks_shared_key = "GbeKTsEzoCAdhoc5J99rsehkFYgrbFTOguegObgSBAYBnwY9REsGX1FE2lZsXH6g/1COwGwu932ARcNA2+GI/g=="

#The log type is the name of the event that is being submitted
log_type = 'TadWebMonitorTest'

#An example JSON web monitor object
json_data = [{
  "slot_ID": 12345,
  "ID": "5cdad72f-c848-4df0-8aaa-ffe033e75d57",
  "availability_Value": 100,
  "performance_Value": 6.954,
  "measurement_Name": "last_one_hour",
  "duration": 3600,
  "warning_Threshold": 0,
  "critical_Threshold": 0,
  "IsActive": "true"
},
{
  "slot_ID": 67890,
  "ID": "b6bee458-fb65-492e-996d-61c4d7fbb942",
  "availability_Value": 100,
  "performance_Value": 3.379,
  "measurement_Name": "last_one_hour",
  "duration": 3600,
  "warning_Threshold": 0,
  "critical_Threshold": 0,
  "IsActive": "false"
}]
body = json.dumps(json_data)

#####################
######Functions######
#####################

#Build the API signature
def build_signature(customer_id, shared_key, date, content_length, method, content_type, resource):
  x_headers = 'x-ms-date:' + date
  string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
  bytes_to_hash = str.encode(string_to_hash,'utf-8')  
  decoded_key = base64.b64decode(shared_key)
  encoded_hash = (base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest())).decode()
  authorization = "SharedKey {}:{}".format(customer_id,encoded_hash)
  return authorization

#Build and send a request to the POST API
def post_data(customer_id, shared_key, body, log_type):
  method = 'POST'
  content_type = 'application/json'
  resource = '/api/logs'
  rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
  content_length = len(body)
  signature = build_signature(customer_id, shared_key, rfc1123date, content_length, method, content_type, resource)
  uri = 'https://' + customer_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

  headers = {
      'content-type': content_type,
      'Authorization': signature,
      'Log-Type': log_type,
      'x-ms-date': rfc1123date
  }

  response = requests.post(uri,data=body, headers=headers)
  if (response.status_code >= 200 and response.status_code <= 299):
      print ('Accepted')
      print(response.url)  
      print(response.status_code)
  else:
      print ("Response code: {}".format(response.status_code))
      
#Post the log
post_data(wks_id, wks_shared_key, body, log_type)