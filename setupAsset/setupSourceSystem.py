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

script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
gbl_cfg_rel_path = "config/globalConfig.json"
gbl_cfg_abs_path = os.path.join(script_dir, gbl_cfg_rel_path)

with open(gbl_cfg_abs_path) as json_file:
  global_config = json.load(json_file)

src_sys_id = int(str(random()).split(".")[1])
def insert_src_sys_item_dynamoDB(src_json_file, region):
  dynamodb = boto3.resource('dynamodb', region_name = region)
  source_system_table = dynamodb.Table('{}.source_system'.format(global_config["fm_prefix"]))

  with open(src_json_file) as json_file:
    src_config = json.load(json_file)

  bucket_name = global_config["fm_prefix"] + "-" + str(src_sys_id) + "-" + region
  src_sys_nm = src_config["src_sys_nm"]
  mechanism = src_config["mechanism"]
  data_owner = src_config["data_owner"]
  support_cntct = src_config["support_cntct"]

  response = source_system_table.put_item(
    Item = {
      'src_sys_id': src_sys_id,
      'bucket_name': bucket_name,
      'src_sys_nm': src_sys_nm,
      'mechanism': mechanism,
      'data_owner': data_owner,
      'support_cntct': support_cntct
    }
  )

def main():
  insert_src_sys_item_dynamoDB(sys.argv[1], global_config["primary_region"])
  insert_src_sys_item_dynamoDB(sys.argv[1], global_config["secondary_region"])

if __name__=="__main__":
  main()
