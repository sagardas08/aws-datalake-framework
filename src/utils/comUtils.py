import json
import decimal
from io import StringIO

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
