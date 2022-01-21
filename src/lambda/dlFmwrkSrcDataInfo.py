import json
import boto3
import time
import os

print('Loading function')

def lambda_handler(event, context):
  message = json.loads(event['Records'][0]['Sns']['Message'])
  bucket = message['Records'][0]['s3']['bucket']['name']
  key = message['Records'][0]['s3']['object']['key']
  my_region = os.environ['AWS_REGION']

  key_path = key[:key.rfind('/')] + "/"
  source_path = "s3://" + bucket + "/" + key_path
  source_id = bucket.split('-')[2]
  asset_id = key.split('/')[0]

  print ("Source Path: {}, Source ID: {}, Asset ID: {}".format(source_path, source_id, asset_id))

  ts = time.time()
  state_machine_name = 'dl-fmwrk-data-pipeline' + str(ts)
  client = boto3.client('stepfunctions')
  response = client.start_execution(
    stateMachineArn='arn:aws:states:{}:076931226898:stateMachine:dl-fmwrk-data-pipeline'.format(my_region),
    name=state_machine_name,
    input= json.dumps({'source_path':source_path, 'source_id':source_id, 'asset_id':asset_id})
  )

  print (response)