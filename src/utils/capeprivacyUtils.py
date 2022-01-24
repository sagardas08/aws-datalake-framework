import json
import cape_privacy as cape
import pandas as pd
from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import ColumnRedact
from cape_privacy.spark.transformations import DatePerturbation
from cape_privacy.spark.transformations import NumericPerturbation
from cape_privacy.spark.transformations import NumericRounding
from cape_privacy.spark.transformations import Tokenizer
from pyspark.sql.functions import col,lit, udf
from cape_privacy.spark.transformations.tokenizer import ReversibleTokenizer
from cape_privacy.spark.transformations.tokenizer import TokenReverser
from pyspark import sql
from pyspark.sql import functions
from datetime import datetime, date
from pyspark.sql import Row
from pyspark.sql import SparkSession
import boto3
from boto3.dynamodb.conditions import Key
import base64
from botocore.exceptions import ClientError
from utils.comUtils import *

def run_data_masking(spark,source_df,metadata):
  #key=get_secret()
  tokenize = Tokenizer()
  for i in metadata:
    for a,b in i.items():
        if a=="req_tokenization" and b==False:
            col_name=i.get("col_nm")
            source_df = source_df.withColumn(col_name, tokenize(functions.col(col_name)))
  return source_df
    
