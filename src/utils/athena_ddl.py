# imports
import time

import boto3
from boto3.exceptions import ResourceNotExistsError
from botocore.exceptions import ClientError

from .logger import log

# Workgroup Hardcoded


@log
def get_or_create_db(region, db_name, logger=None):
    """
    Utility method to create a Database if one doesn't exist

    :param logger: Logging object to record details
    :param region: AWS region
    :param db_name: The name by which a DB is supposed to be created
    :return: None
    """
    client = boto3.client("athena", region_name=region)
    try:
        response = client.get_database(
            CatalogName="AwsDataCatalog", DatabaseName="university"
        )
        if "Database" in response.keys():
            if response["Database"]["Name"] == db_name:
                logger.write(message=f"The database: {db_name} exists")
            else:
                print(f"Attempting to create the db: {db_name}")
                query = f"create database {db_name}"
                client.start_query_execution(QueryString=query, WorkGroup="dl-fmwrk")
        else:
            logger.write(message=f"Invalid response: {response}")
    except Exception as e:
        logger.write(message=e)
        logger.write(message=f"Attempting to create the db: {db_name}")
        query = f"create database {db_name}"
        client.start_query_execution(QueryString=query, WorkGroup="dl-fmwrk")


def generate_ddl(df, db, table, path, partition, encrypt):
    """
    Dynamic generation of Athena DDL on the go based on the dataframe object.

    :param df: The dataframe based on which the Athena DDL is to be constructed
    :param db: The DB in which the query needs to be executed
    :param table: The table that will be created post execution
    :param path: The path to underlying S3 files in the target system
    :param partition: Flag to set partition True / False
    :param encrypt: Flag to enable / disable encryption of underlying dataset.
    :return: string
    """
    # Get the data types of the different columns
    fields = df.dtypes
    # If the location path is of the s3a format, change it to s3
    loc_path = "LOCATION " + f"'{path.replace('s3a', 's3')}'"
    # dynamically add fields to the schema
    schema = ""
    for field in fields:
        column = field[0]
        datatype = field[1]
        schema += f"`{column}` {datatype},"
    schema = schema.rstrip(",")
    # setting the Table properties
    encryption = "false" if not encrypt else "true"
    # TODO: Partition is currently hardcoded. Add a partition logic
    partition_string = f"PARTITIONED BY (partition_instance bigint)"
    row_format = "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' "
    input_format = "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "
    output_format = (
        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
    )
    tbl_prop = f"TBLPROPERTIES ('has_encrypted_data'='{encryption}')"
    # creating the dynamic sql statement
    statement = (
        f"CREATE EXTERNAL TABLE `{db}`.`{table}`({schema}) {row_format} "
        f"{input_format} {output_format} {loc_path} {tbl_prop}"
    )
    # if partition is enabled, currently supported to a single partition column
    if partition:
        statement = (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table}`({schema}) "
            f"{partition_string} {row_format} {input_format} {output_format} {loc_path} {tbl_prop}"
        )
    return statement


def exists_query(client, table, exec_id):
    """
    returns if a table exists in the said DB or not
    """
    query_result = client.get_query_results(QueryExecutionId=exec_id)
    result_set = query_result["ResultSet"]
    rows = result_set["Rows"]
    row_elements = len(rows)
    exists = False
    if not rows:
        return exists
    elif row_elements >= 1:
        if table in [x["Data"][0]["VarCharValue"] for x in rows]:
            exists = True
    return exists


def check_table_exists(client, db, table):
    """
    Utility method to check if a table exists or not in the database.
    :param client: The boto3 Client
    :param db: The database under which the query is supposed to run
    :param table: The table to be searched for
    :return: bool
    """
    exists = None
    try:
        response = client.get_table_metadata(
            CatalogName="AwsDataCatalog", DatabaseName=db, TableName=table
        )
        exists = True
    except ClientError as e:
        exists = False
    assert exists is not None
    return exists


@log
def get_or_create_table(
    region, df, target_info, asset_id, path, partition=False, encrypt=False, logger=None
):
    """
    Create a table in Athena under the specified DB.
    :param logger: The logger object to record details of the function
    :param region: The AWS region
    :param df: The Spark Dataframe object
    :param target_info: A dict of target system information
    :param path: The path to the AWS target system.
    :param asset_id: The asset id
    :param partition: Boolen flag to enable or disable partition
    :param encrypt: Boolean flag to enable or disable encryption
    :return:
    """
    ath = boto3.client("athena", region_name=region)
    db = target_info["domain"]
    table = target_info["subdomain"] + "_" + asset_id
    # check if the table exists on Athena
    table_exists = check_table_exists(ath, db, table)
    if not table_exists:
        logger.write(message=f"The table: {db}.{table} does not exist.")
        ddl = generate_ddl(df, db, table, path, partition, encrypt)
        # TODO: remove hardcoded WorkGroup values
        ath.start_query_execution(QueryString=ddl, WorkGroup="dl-fmwrk")
    elif table_exists:
        logger.write(message=f"The table: {db}.{table} exists.")


@log
def manage_partition(
    region, target_info, asset_id, partition_instance, location, logger=None
):
    """
    Add partitions to the created Athena Table.

    :param logger:
    :param region:
    :param target_info:
    :param asset_id:
    :param partition_instance:
    :param location:
    :return:
    """
    partition_location = location.replace("s3a", "s3")
    ath = boto3.client("athena", region_name=region)
    db = target_info["domain"]
    table = target_info["subdomain"] + "_" + asset_id
    # Alter table statement
    alter_table = f"""
    ALTER TABLE {db}.{table} ADD IF NOT EXISTS 
    PARTITION (partition_instance='{partition_instance}')
    LOCATION '{partition_location}';
    """
    logger.write(message=f"Managing the partitions using {alter_table}")
    # Execute the partition statement on Athena (Workgroup Hardcoded)
    ath.start_query_execution(QueryString=alter_table, WorkGroup="dl-fmwrk")
