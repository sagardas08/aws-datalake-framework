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


@log
def update_data_catalog(
    table_name,
    exec_id,
    dq_validation=None,
    data_masking=None,
    data_standardization=None,
    logger=None,
):
    """

    :param table_name:
    :param exec_id:
    :param dq_validation:
    :param data_masking:
    :param data_standardization:
    :param logger:
    :return:
    """
    table = boto3.resource("dynamodb").Table(table_name)
    response = table.get_item(Key={"exec_id": exec_id})
    item = response["Item"]
    if dq_validation:
        item["dq_validation"] = dq_validation
    elif data_masking:
        item["data_masking"] = data_masking
    elif data_standardization:
        item["data_standardization"] = data_standardization
    table.put_item(Item=item)


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
    :param logger:
    :return:
    """
    source_df = None
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
        assert source_df is not None
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
        temp_dict = dict()
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
    if num_fails >= 1:
        if logger:
            logger.write(message=f"Found {num_fails} Failures in the source file.")
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
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket(bucket)
    source = "/".join(path.split("/")[3:])
    target = path.split("/")[3] + "/Errors"
    for obj in s3_bucket.objects.filter(Prefix=source):
        source_filename = obj.key.split("/")[-1]
        print(obj.key)
        print(source_filename)
        copy_source = {"Bucket": bucket, "Key": obj.key}
        target_filename = "{}/{}".format(target, source_filename)
        s3_bucket.copy(copy_source, target_filename)
        s3.Object(bucket, obj.key).delete()


@log
def move_source_file(path, dq_result=None, schema_validation=None, logger=None):
    """
    :param path:
    :param dq_result:
    :param schema_validation:
    :param logger:
    :return:
    """
    if dq_result is not None:
        # check for 'Failure' in the DQ results
        failure = check_failure(dq_result, logger)
        if failure:
            # if there is even a single DQ fail then move the source file
            logger.write(message="Attempting to move the file")
            move_file(path, logger)
        else:
            if logger:
                logger.write(message="No failures found")
    elif not schema_validation:
        # in case the schema is not validated, move the source file to error location
        move_file(path, logger)
        if logger:
            logger.write(
                message="Moving the file to Error location due to schema irregularities"
            )
