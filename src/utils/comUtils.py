import base64
import json
import decimal
from datetime import datetime, timedelta
from io import StringIO
from boto3.dynamodb.conditions import Key
import boto3
import pydeequ
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

from .logger import log


def get_current_time():
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return timestamp

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
            logger.write(message=f"Found {num_fails} Failure(s) in the source file.")
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

@log
def store_sparkdf_to_s3(dataframe, target_path, asset_file_type, asset_file_delim, asset_file_header, logger=None):
    """
    utility method to store a Spark dataframe to S3 bucket
    :param dataframe: The spark dataframe
    :param target_path: The S3 URI
    :param asset_file_type: Type of the file that the dataframe should be written as
    :param asset_file_delim: The delimiter
    :param asset_file_header: The header true/false
    :return:
    """
    target_path = target_path.replace("s3://", "s3a://")
    timestamp = get_current_time()
    target_path = target_path + timestamp + "/"
    if asset_file_type == 'csv':
        dataframe.repartition(1).write.csv(target_path, header=True, mode="overwrite")
    if asset_file_type == 'parquet':
        dataframe.repartition(1).write.parquet(target_path, header=True, mode="overwrite")
    if asset_file_type == 'json':
        dataframe.repartition(1).write.json(target_path, header=True, mode="overwrite")
    if asset_file_type == 'orc':
        dataframe.repartition(1).write.orc(target_path, header=True, mode="overwrite")


@log
def get_secret(secretname, regionname, logger=None):
    """
    Utility function to get secret key from secrets manager for tokenising in data masking
    :param secretname: The name of the secret key that is used for tokenisazation
    :param regionname: The AWS region for e.g. us-east-1
    :return:
    """
    secret_name = secretname
    region_name = regionname

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId = secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion
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
            key_value_pair = json.loads(secret)
            key = key_value_pair["key"]
            if logger:
                logger.write(message="Successfully retrieved the key from secret manager")
            return key
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

def get_timestamp(source_path):
    return source_path.split("/")[5]


@log
def get_target_system_info(fm_prefix, target_id, region, logger=None):
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = f"{fm_prefix}.target_system"
    logger.write(message=f"Getting asset info from {table}")
    target_system_info = dynamodb.Table(table)
    target_system_items = target_system_info.query(
        KeyConditionExpression=Key("tgt_sys_id").eq(int(target_id))
    )
    target_items = dynamodbJsonToDict(target_system_items)
    return target_items


@log
def get_standardization_path(target_system_info, asset_id, timestamp, logger=None):
    target_bucket_name = target_system_info["bucket_name"]
    target_subdomain = target_system_info["subdomain"]
    return f"s3a://{target_bucket_name}/{target_subdomain}/{asset_id}/{timestamp}/"
