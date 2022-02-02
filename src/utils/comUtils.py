import json
import decimal
from datetime import datetime, timedelta
from io import StringIO

import boto3
import pydeequ
from pyspark.sql import SparkSession

from .logger import log


def get_current_time():
    time_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    time_str = time_ist.strftime("%d-%m-%y %H:%M:%S")
    return time_str


def get_global_config():
    config_file_path = "config/globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    return config


@log
def get_spark(logger=None):
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


@log
def create_spark_df(
    spark,
    source_file_path,
    asset_file_type,
    asset_file_delim,
    asset_file_header,
    logger=None,
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


@log
def get_metadata(table, region, logger=None):
    """
    Get the metadata from dynamoDB to find which checks to run for which columns
    :param logger:
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


@log
def check_failure(dataframe, logger):
    """

    :param dataframe:
    :param logger:
    :return:
    """
    df_fail = dataframe.filter(dataframe.constraint_status != "Success")
    num_fails = df_fail.count()
    if num_fails > 1:
        if logger:
            logger.write(
                message=f"Found {num_fails} Failures. Attempting to move the file"
            )
        return True
    return False


@log
def move_file(path, logger=None):
    """

    :param path:
    :param logger:
    :return:
    """
    word_list = path.split("/")
    bucket = word_list[2]
    file_name = word_list[-1]
    source_key = "/".join(word_list[3:])
    curr_time = get_current_time()
    destination_key = word_list[3] + f"/Error/{curr_time}/{file_name}"
    copy_source = {"Bucket": bucket, "Key": source_key}
    s3 = boto3.resource("s3")
    s3.meta.client.copy(copy_source, bucket, destination_key)
    s3.Object(bucket, source_key).delete()
    if logger is not None:
        logger.write(message=f"Moved the file {file_name} to {destination_key}")


@log
def move_source_file(
    source_file_path, dq_result=None, schema_validation=None, logger=None
):
    """

    :param source_file_path:
    :param dq_result:
    :param schema_validation:
    :param logger:
    :return:
    """
    if dq_result is not None:
        failure = check_failure(dq_result, logger)
        if failure:
            move_file(source_file_path, logger)
        else:
            if logger:
                logger.write(message="No failures found.")
    elif not schema_validation:
        move_file(source_file_path, logger)
        if logger:
            logger.write(
                message="Moving the file to Error location due to schema irregularities"
            )
