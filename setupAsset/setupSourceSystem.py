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

src_sys_id = int(str(random()).split(".")[1])
def insert_src_sys_item_dynamoDB(src_json_file, region):
  global_config = getGlobalParams()
  dynamodb = boto3.resource('dynamodb', region_name = region)
  source_system_table = dynamodb.Table('{}.source_system'.format(global_config["fm_prefix"]))

  with open(src_json_file) as json_file:
    src_config = json.load(json_file)

  bucket_name = global_config["fm_prefix"] + "-" + str(src_sys_id) + "-" + region
  src_sys_nm = src_config["src_sys_nm"]
  mechanism = src_config["mechanism"]
  data_owner = src_config["data_owner"]
  support_cntct = src_config["support_cntct"]

  print("Insert source system info in {}.source_system table".format(global_config["fm_prefix"]))
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

def run_aws_cft(src_json_file, region):
  global_config = getGlobalParams()
  src_sys_cft = 'cft/sourceSystem.yaml'
  with open(src_sys_cft) as yaml_file:
    template_body = yaml_file.read()

  print("Setup source system flow through {}-{}-{} stack".format(global_config["fm_prefix"], str(src_sys_id), region))
  stack = boto3.client('cloudformation', region_name = region)
  response = stack.create_stack(
    StackName = global_config["fm_prefix"] + "-" + str(src_sys_id) + "-" + region,
    TemplateBody = template_body,
    Parameters=[
      {
        'ParameterKey': 'CurrentRegion',
        'ParameterValue': region
      },
      {
        'ParameterKey': 'DlFmwrkPrefix',
        'ParameterValue': global_config["fm_prefix"]
      },
      {
        'ParameterKey': 'AwsAccount',
        'ParameterValue': global_config["aws_account"]
      },
      {
        'ParameterKey': 'srcSysId',
        'ParameterValue': str(src_sys_id)
      },
    ],
  )

def main():
  global_config = getGlobalParams()
  insert_src_sys_item_dynamoDB(sys.argv[1], global_config["primary_region"])
  insert_src_sys_item_dynamoDB(sys.argv[1], global_config["secondary_region"])
  run_aws_cft(sys.argv[1], global_config["primary_region"])
  run_aws_cft(sys.argv[1], global_config["secondary_region"])

if __name__=="__main__":
  main()
