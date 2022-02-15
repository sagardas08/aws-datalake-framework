import json
import boto3
import datetime
import os

print("Loading function")


def lambda_handler(event, context):
  message = json.loads(event['Records'][0]['Sns']['Message'])
  bucket = message['Records'][0]['s3']['bucket']['name']
  key = message['Records'][0]['s3']['object']['key']
  my_region = os.environ['AWS_REGION']
  fm_prefix = os.environ['fm_prefix']
  aws_account = os.environ['aws_account']

  key_path = key[: key.rfind("/")] + "/"
  source_path = "s3://" + bucket + "/" + key_path
  source_id = bucket.split("-")[2]
  asset_id = key.split("/")[0]
  print(
    "Source Path: {}, Source ID: {}, Asset ID: {}".format(
      source_path, source_id, asset_id
    )
  )

  now = datetime.datetime.now()
  ts = now.strftime('%Y%m%d%H%M%S')
  exec_id = source_id + "_" + asset_id + "_" + str(ts)
  state_machine_name = fm_prefix + '-data-pipeline' + str(ts)

  dynamodb = boto3.resource('dynamodb', region_name = my_region)
  source_system_table = dynamodb.Table('{}.data_catalog.{}'.format(fm_prefix, asset_id))
  print("Insert data catalog info in {}.data_catalog.{} table".format(fm_prefix, asset_id))
  response = source_system_table.put_item(
    Item = {
      'exec_id': exec_id,
      'proc_start_ts': str(ts),
      'source_path': source_path,
      'dq_validation': 'not started',
      'data_masking': 'not started',
      'data_standardization': 'not started'
    }
  )

  client = boto3.client('stepfunctions')
  response = client.start_execution(
    stateMachineArn='arn:aws:states:{}:{}:stateMachine:{}-data-pipeline'.format(my_region, aws_account, fm_prefix),
    name=state_machine_name,
    input= json.dumps({'source_path':source_path, 'source_id':source_id, 'asset_id':asset_id, 'exec_id':exec_id})
  )

  print (response)
  
