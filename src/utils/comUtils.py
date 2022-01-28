import json
import decimal
from io import StringIO
 from datetime import datetime
import boto3
import pydeequ
from pyspark.sql import SparkSession


def get_spark():
    """
    Utility method to return a spark Session object initialized with Pydeequ jars
    :return: Spark Session Object
    """
    spark = (
        SparkSession.builder.config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )
    return spark


def stop_spark(spark):
    """
    Utility method to stop a Spark Session
    :param spark: Spark Session Object
    :return: None
    """
    spark.sparkContext._gateway.close()
    spark.stop()
    print("Stopping Spark Session")


def create_spark_df(
    spark, source_file_path, asset_file_type, asset_file_delim, asset_file_header
):
    """
    :param spark:
    :param source_file_path:
    :param asset_file_type:
    :param asset_file_delim:
    :param asset_file_header:
    :return:
    """
    if asset_file_type == "csv" and asset_file_header == True:
        source_df = spark.read.csv(
            path=source_file_path, sep=asset_file_delim, header=True, inferSchema=True
        )
    elif asset_file_type == "csv" and asset_file_header == False:
        source_df = spark.read.csv(path=source_file_path, sep=asset_file_delim)
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
    for i in dynamodbJson["Items"]:
        items = json.dumps(i, cls=DecimalEncoder)
    return json.loads(items)


# utility function to store Pandas DF to S3
def store_to_s3(data_type, bucket, key, data):
    """
    utility method to store a Pandas dataframe to S3 bucket
    :param data_type: The data type - Pandas / Spark
    :param bucket: The S3 bucket
    :param key: The final name of the file
    :param data: the dataframe object
    :return:
    """
    if data_type == "json":
        s3 = boto3.client("s3")
        json_object = data
        s3.put_object(Body=json.dumps(json_object), Bucket=bucket, Key=key)
    elif data_type == "dataframe":
        csv_buffer = StringIO()
        data.to_csv(csv_buffer)
        s3_resource = boto3.resource("s3")
        try:
            s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
            print("stored DF to bucket -> {0} with key -> {1}".format(bucket, key))
        except Exception as e:
            print(e)


def get_metadata(table, region="us-east-1"):
    """
    Get the metadata from dynamoDB to find which checks to run for which columns
    :param table: The DynamoDB table name
    :param region: The AWS region for e.g. us-east-1
    :return:
    """
    temp_dict = dict()
    response_list = list()
    client = boto3.client("dynamodb", region_name=region)
    response = client.scan(TableName=table)["Items"]
    for item in response:
        for k, v in item.items():
            temp_dict[k] = list(v.values())[0]
        response_list.append(temp_dict)
        dct = {}
    return response_list
  
#utility method to store a Spark dataframe to S3 bucket
def store_sparkdf_to_s3(dataframe,target_path,asset_file_type,asset_file_delim,asset_file_header):
   """
    
    :param dataframe: The spark dataframe
    :param target_path: The S3 URI
    :param asset_file_type: Type of the file that the dataframe should be written as
    :param asset_file_delim: The delimiter
    :param asset_file_header: The header true/false
    :return:
    """
  target_path = target_path.replace("s3://", "s3a://")
  timestamp = str(datetime.now())
  splitlist=timestamp.split(".")
  timestamp=splitlist[0]
  timestamp=timestamp.replace(" ","").replace(":","").replace("-","")
  target_path=target_path+timestamp+"/"
  if asset_file_type=='csv':
    dataframe.coalesce(1).write.option("header",asset_file_header).option("delimiter",asset_file_delim).csv(target_path)
  if asset_file_type=='parquet':
    dataframe.coalesce(1).write.parquet(target_path)
  if asset_file_type=='json':
    dataframe.coalesce(1).write.json(target_path)
  if asset_file_type=='orc':
    dataframe.coalesce(1).write.orc(target_path)
    
    
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
  

