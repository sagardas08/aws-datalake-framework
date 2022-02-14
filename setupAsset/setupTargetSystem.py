'''
== Create a new source system
1. Insert a new record in the dynamoDb table
2. Create an S3 bucket associated to the new source system
3. Create a new IAM user (or othe mechanism) to access the S3 bucket
4. Create a bucket policy accordingly and associate with the S3 bucket
5. Create an SNS topic associated to the S3 bucket
6. Subscribe the SNS topic to the DL source system lambda function
''' 

import boto3
import json
import decimal
import time
import sys
import os
from random import random
from utils.comUtils import getGlobalParams

tgt_sys_id = int(str(random()).split(".")[1])
def insert_tgt_sys_item_dynamoDB(tgt_json_file, region):
  global_config = getGlobalParams()
  dynamodb = boto3.resource('dynamodb', region_name = region)
  source_system_table = dynamodb.Table('{}.target_system'.format(global_config["fm_prefix"]))

  with open(tgt_json_file) as json_file:
    tgt_config = json.load(json_file)

  bucket_name = global_config["fm_tgt_prefix"] + "-" + tgt_config["domain"] + "-" + region
  db_name = global_config["fm_tgt_prefix"] + "_" + tgt_config["domain"]
  data_owner = tgt_config["data_owner"]
  support_cntct = tgt_config["support_cntct"]
  domain = tgt_config["domain"]
  subdomain = tgt_config["subdomain"]

  print("Insert target system info in {}.target_system table".format(global_config["fm_prefix"]))
  response = source_system_table.put_item(
    Item = {
      'tgt_sys_id': tgt_sys_id,
      'bucket_name': bucket_name,
      'data_owner': data_owner,
      'support_cntct': support_cntct,
      'domain': domain,
      'subdomain': subdomain
    }
  )

def main():
  global_config = getGlobalParams()
  insert_tgt_sys_item_dynamoDB(sys.argv[1], global_config["primary_region"])
  insert_tgt_sys_item_dynamoDB(sys.argv[1], global_config["secondary_region"])

if __name__=="__main__":
  main()
