import base64
import json
import decimal
from datetime import datetime, timedelta
from io import StringIO
import boto3
import pydeequ
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

from .logger import log


def get_current_time():
    """
    current time in format YYYMMddHHMMSS
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return timestamp


@log
def update_data_catalog(
    conn,
    table_name,
    exec_id,
    dq_validation=None,
    data_masking=None,
    data_publish=None,
    logger=None,
):
    # method to update the data catalog entry
    """
    :param conn: DB connection object
    :param table_name: Name of the table
    :param exec_id: ID of each execution
    :param dq_validation: Status of dq_validation
    :param data_masking: Status of data_masking
    :param data_publish: Status of data_publish
    :param logger: Logger object
    :return: None
    """
    item = {}
    if dq_validation:
        item["dq_validation"] = dq_validation
    elif data_masking:
        item["data_masking"] = data_masking
    elif data_publish:
        item["data_publish"] = data_publish
    where_clause = ("exec_id=%s", [exec_id])
    conn.update(table=table_name, data=item, where=where_clause)


@log
def get_spark(logger=None):
    """
    Utility method to return a spark Session object initialized with Pydeequ jars
    :param logger: Logger object
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
    # Generates a spark dataframe from the source file based
    # on the different file conditions as specified in the asset info
    """
    :param spark: Spark session object
    :param source_file_path: S3 uri
    :param asset_file_type: Type of file eg.CSV,JSON
    :param asset_file_delim: Delimiter eg.','
    :param asset_file_header: Boolean
    :param logger: Logger object
    :return: Spark dataframe
    """
    source_df = None
    if asset_file_type == "csv" and asset_file_header == True:
        source_df = spark.read.csv(
            path=source_file_path,
            sep=asset_file_delim,
            header=True,
            inferSchema=True
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


@log
def get_metadata(conn, asset_id, logger=None):
    """
    Get the metadata from DB to find which checks to run for which columns
    :param conn: Connection object
    :param asset_id: Asset ID
    :param logger: Logger object
    :return : Dictionary
    """
    return conn.retrieve_dict(
        "data_asset_attributes", cols="all", where=("asset_id=%s", [asset_id])
    )


@log
def check_failure(dataframe, logger):
    # method to check if any of the constraint checks has failed
    """
    :param dataframe: Spark dataframe
    :param logger: logger object
    :return: Boolean
    """
    # set the failure flag to false initially
    fail = False
    # filter the incoming dq results on the basis of constraint status
    df_fail = dataframe.filter(dataframe.constraint_status != "Success")
    # number of failures is the count of the filtered df
    num_fails = df_fail.count()
    # if there is a single / multiple failures, set the failure flag to true
    if num_fails >= 1:
        if logger:
            logger.write(message=f"Found {num_fails} Failure(s) in the source file.")
        fail = True
    return fail


@log
def move_file(path, logger=None):
    # utility method to move a file if a failure is encountered
    """
    :param path: Path of file
    :param logger: Logger object
    :return: None
    """
    # extracting the bucket name
    split_path = path.split("/")
    bucket = split_path[2]
    # create a bucket resource
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket(bucket)
    source = "/".join(path.split("/")[3:])
    target = path.split("/")[3] + "/error"
    # move the file by narrowing the search
    for obj in s3_bucket.objects.filter(Prefix=source):
        source_filename = obj.key.split("/")[-1]
        copy_source = {"Bucket": bucket, "Key": obj.key}
        target_filename = "{}/{}".format(target, source_filename)
        # copy and delete method is used since there is no default move option
        s3_bucket.copy(copy_source, target_filename)
        s3.Object(bucket, obj.key).delete()


@log
def move_source_file(path, dq_result=None, schema_validation=None, logger=None):
    """
    main method to move a source file in case of invalid schema / dq failure
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
def store_sparkdf_to_s3(
    dataframe,
    target_path,
    asset_file_type,
    asset_file_delim,
    asset_file_header,
    logger=None,
):
    """
    utility method to store a Spark dataframe to S3 bucket
    :param dataframe: The spark dataframe
    :param target_path: The S3 URI
    :param asset_file_type: Type of the file that the dataframe should be written as
    :param asset_file_delim: The delimiter
    :param asset_file_header: The header true/false
    :param logger: Logger object
    :return:
    """
    target_path = target_path.replace("s3://", "s3a://")
    if asset_file_type == "csv" and asset_file_header:
        dataframe.repartition(1).write.csv(target_path, header=True, mode="overwrite")
    else:
        dataframe.repartition(1).write.csv(target_path, mode="overwrite")
    if asset_file_type == "parquet":
        dataframe.repartition(1).write.parquet(
            target_path, mode="overwrite"
        )
    if asset_file_type == "json":
        dataframe.repartition(1).write.json(target_path, mode="overwrite")
    if asset_file_type == "orc":
        dataframe.repartition(1).write.orc(target_path, mode="overwrite")


@log
def get_secret(secret_nm, region_nm, logger=None):
    """
    Utility function to get secret key from secrets manager for tokenizing in data masking
    :param secret_nm: The name of the secret key that is used for tokenization
    :param region_nm: The AWS region for e.g. us-east-1
    :param logger:Logger object
    :return: Key for data masking
    """
    secret_name = secret_nm
    region_name = region_nm

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "DecryptionFailureException":
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InternalServiceErrorException":
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion
            raise e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "ResourceNotFoundException":
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            key_value_pair = json.loads(secret)
            key = key_value_pair["key"]
            if logger:
                logger.write(
                    message="Successfully retrieved the key from secret manager"
                )
            return key
        else:
            decoded_binary_secret = base64.b64decode(
                get_secret_value_response["SecretBinary"]
            )


def get_timestamp(source_path):
    """
    Utility function to get timestamp from source path
    :param source_path: The s3 uri
    :return: Timestamp as string
    """
    return source_path.split("/")[5]


@log
def get_target_system_info(conn, target_id, logger=None):
    """
    Utility function to get the items from target system

    :param conn: DB connection object
    :param target_id: The id of target
    :param logger: Logger object
    :return: Dictionary
    """
    tgt_sys_dict = conn.retrieve_dict(
        "target_system", cols="all", where=("target_id=%s", [target_id])
    )
    logger.write(message="Attempting to get target system information")
    return tgt_sys_dict[0]


@log
def get_publish_path(target_system_info, asset_id, timestamp, logger=None):
    """
    Utility function to get the path where the standardized file should be stored
    :param target_system_info: Dictionary containing target system info
    :param asset_id: Asset id
    :param timestamp: The timestamp
    :param logger: logger object
    :return: s3 uri
    """
    target_bucket_name = target_system_info["bucket_name"]
    target_subdomain = target_system_info["subdomain"]
    return f"s3://{target_bucket_name}/{target_subdomain}/{asset_id}/{timestamp}/"


def get_athena_path(target_system_info, asset_id):
    """
    Utility function to get the path for the specified athena table
    :param target_system_info: Dictionary containing target system info
    :param asset_id: Asset id
    :return: s3 uri
    """
    target_bucket_name = target_system_info["bucket_name"]
    target_subdomain = target_system_info["subdomain"]
    return f"s3a://{target_bucket_name}/{target_subdomain}/{asset_id}/"