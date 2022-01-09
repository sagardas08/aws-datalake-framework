import json, ast, decimal
from io import StringIO

import boto3
from boto3.dynamodb.conditions import Key
from pyspark.sql import SparkSession, Row

import pydeequ
from pydeequ.analyzers import *
from pydeequ.profiles import *
from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.repository import *

import sys
from awsglue.utils import getResolvedOptions

def get_spark():
  spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())
  return spark

def create_spark_df(
  spark, source_file_path, asset_file_type,
  asset_file_delim, asset_file_header
):
  if asset_file_type == 'csv' and asset_file_header == True:
    source_df = spark.read.csv(
      path=source_file_path, sep = asset_file_delim,
      header=True,  inferSchema=True
    )
  elif asset_file_type == 'csv' and asset_file_header == False:
    source_df = spark.read.csv(
      path=source_file_path, 
      sep = asset_file_delim
    )
  elif asset_file_type == "parquet":
    source_df = spark.read.parquet(source_file_path)
  elif asset_file_type == "json":
    source_df = spark.read.json(source_file_path)
  elif asset_file_type == "orc":
    source_df = spark.read.orc(source_file_path)
  try:
    assert source_df != None
    return source_df
  except AssertionError:
    print("Unable to read the data from the source")
    
# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
  def default(self, o):
    if isinstance(o, decimal.Decimal):
      return str(o)
    if isinstance(o, set):  #<---resolving sets as lists
      return list(o)
    return super(DecimalEncoder, self).default(o)

# utility function that returns spark object

def main():
  spark = get_spark()

  '''
  Capturing source information parameters from the step function. Parameters:
    1. source_path: S3 path where the source file is created/landed
    2. source_id: Source system identifier which is a random number associated
      to the onboarded source. This identifier is also suffixed in the S3 
      bucket name.
    3. asset_id: Asset Identifier which is a random number associated to the 
      onboadred data asset.
  '''
  args = getResolvedOptions(sys.argv, ['source_path', 'source_id', 'asset_id'])
  source_path = args['source_path']
  source_id = args['source_id']
  asset_id = args['asset_id']

  '''
  Pulling the data asset information from dynamoDB table dl_fmwrk.data_asset 
  based on the Asset ID
  '''
  dynamodb = boto3.resource('dynamodb')
  asset_info = dynamodb.Table('dl_fmwrk.data_asset')
  asset_info_items = asset_info.query(
    KeyConditionExpression=Key('asset_id').eq(int(asset_id))
  )
  print("Checkpoint!!!!!!")
  for i in asset_info_items['Items']:
    items = json.dumps(i, cls=DecimalEncoder)
    
  asset_file_type = json.loads(items)['file_type']
  asset_file_delim = json.loads(items)['file_delim']
  asset_file_header = json.loads(items)['file_header']
  print(asset_file_type, "|", asset_file_delim, "|", asset_file_header)

  '''
  Create dataframe using the source data asset
  '''
  source_file_path = source_path.replace("s3://", "s3a://")
  source_df = create_spark_df(
    spark,
    source_file_path,
    asset_file_type,
    asset_file_delim,
    asset_file_header
  )
  source_df.printSchema()
  #col_metadata_tbl = "dl_fmwrk.data_asset." + asset_id

if __name__ == '__main__':
    main()
