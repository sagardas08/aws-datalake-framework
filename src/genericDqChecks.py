import json, ast, decimal, sys, logging

from utils.comUtils import *
from utils.dqUtils import *
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
  dynamodb = boto3.resource('dynamodb', region_name = "us-east-2")
  asset_info = dynamodb.Table('dl_fmwrk.data_asset')
  asset_info_items = asset_info.query(
    KeyConditionExpression=Key('asset_id').eq(int(asset_id))
  )
  items = dynamodbJsonToDict(asset_info_items)
    
  asset_file_type = items['file_type']
  asset_file_delim = items['file_delim']
  asset_file_header = items['file_header']

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

  check_df = run_constraint_checks(spark, source_df)
  source_df.printSchema()
  spark.sparkContext._gateway.close()
  spark.stop()

if __name__ == '__main__':
    main()
