import json
import decimal
import boto3
import pydeequ
from pyspark.sql import SparkSession
import base64
from botocore.exceptions import ClientError

# utility function that returns spark object
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
      path=source_file_path, sep=asset_file_delim,
      header=True,  inferSchema=True
    )
  elif asset_file_type == 'csv' and asset_file_header == False:
    source_df = spark.read.csv(
      path=source_file_path,
      sep=asset_file_delim
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
    if isinstance(o, set):  # <---resolving sets as lists
      return list(o)
    return super(DecimalEncoder, self).default(o)


def dynamodbJsonToDict(dynamodbJson):
  for i in dynamodbJson['Items']:
    items = json.dumps(i, cls=DecimalEncoder)
  return json.loads(items)


# utility function to store Pandas DF to S3
def store_to_s3(data_type, bucket, key, data):
  if data_type == 'json':
    s3 = boto3.client('s3')
    json_object = data
    s3.put_object(
        Body=json.dumps(json_object),
        Bucket=bucket,
        Key=key
    )
  elif data_type == 'dataframe':
    csv_buffer = StringIO()
    data.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    try:
      s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
      print(
        "stored DF to bucket -> {0} with key -> {1}".format(bucket, key))
    except Exception as e:
      print(e)
      
#Utility function to get secret key from secrets manager for tokenising in data masking      
def get_secret():

    secret_name = "cape_privacy_key"
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            key_value_pair=json.loads(secret)
            key=key_value_pair["key"]
            return key
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
