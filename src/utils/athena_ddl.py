# imports
import time

import boto3


def get_or_create_db(region, db_name):
    """

    :param region:
    :param db_name:
    :return:
    """
    ath = boto3.client("athena", region_name=region)
    response = ath.list_databases(CatalogName='AwsDataCatalog')
    time.sleep(2)
    databases = [i['Name'] for i in response['DatabaseList']]
    if db_name in databases:
        print(f"Database: {db_name} already exists")
    else:
        print(f"Database: {db_name} doesn't exist. Creating a new database")
        query = f"create database {db_name}"
        # (Workgroup Hardcoded)
        ath.start_query_execution(
            QueryString=query,
            WorkGroup='dl-fmwrk',
        )


def generate_ddl(df, db, table, path, partition, encrypt):
    """

    :param df:
    :param db:
    :param table:
    :param path:
    :param partition:
    :param encrypt:
    :return:
    """

    fields = df.dtypes
    loc_path = "LOCATION " + f"'{path.replace('s3a', 's3')}'"
    schema = ""
    for field in fields:
        column = field[0]
        datatype = field[1]
        schema += f"`{column}` {datatype},"
    schema = schema.rstrip(",")
    encryption = 'false' if not encrypt else 'true'
    # Partition is currently hardcoded. TODO: Add a partition logic
    partition_string = f"PARTITIONED BY (partition_instance bigint)"
    row_format = "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' "
    input_format = "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "
    output_format = "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
    tbl_prop = f"TBLPROPERTIES ('has_encrypted_data'='{encryption}')"
    statement = f"CREATE EXTERNAL TABLE `{db}`.`{table}`({schema}) {row_format} " \
                f"{input_format} {output_format} {loc_path} {tbl_prop}"
    if partition:
        statement = f"CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table}`({schema}) " \
                    f"{partition_string} {row_format} {input_format} {output_format} {loc_path} {tbl_prop}"
    return statement


def exists_query(client, table, exec_id):
    """

    :param client:
    :param table:
    :param exec_id:
    :return:
    """
    query_result = client.get_query_results(QueryExecutionId=exec_id)
    result_set = query_result['ResultSet']
    rows = result_set['Rows']
    row_elements = len(rows)
    exists = False
    if not rows:
        return exists
    elif row_elements >= 1:
        if table in [x['Data'][0]['VarCharValue'] for x in rows]:
            exists = True
    return exists


def check_table_exists(client, db, table):
    """

    :param client:
    :param db:
    :param table:
    :return:
    """
    # (Workgroup Hardcoded)
    response = client.start_query_execution(
        QueryString=f"SHOW TABLES IN {db} '*{table}*'", WorkGroup='dl-fmwrk'
    )
    # Athena Waiter is not implemented in boto3, hence adding a delay
    time.sleep(5)
    exec_id = response['QueryExecutionId']
    table_exists = exists_query(client, table, exec_id)
    return table_exists


def get_or_create_table(region, df, target_info, asset_id,
                        path, partition=False, encrypt=False):
    """

    :param region:
    :param df:
    :param target_info:
    :param path:
    :param asset_id:
    :param partition:
    :param encrypt:
    :return:
    """
    ath = boto3.client('athena', region_name=region)
    db = target_info['domain']
    table = target_info['subdomain'] + "_" + asset_id
    # check if the table exists on Athena
    table_exists = check_table_exists(ath, db, table)
    if not table_exists:
        print(f"The table: {db}.{table} does not exist.")
        ddl = generate_ddl(df, db, table, path, partition, encrypt)
        # (Workgroup Hardcoded)
        ath.start_query_execution(QueryString=ddl, WorkGroup='dl-fmwrk')
    elif table_exists:
        print(f"The table: {db}.{table} exists.")


def manage_partition(region, target_info, asset_id, partition_instance, location):
    """

    :param region: 
    :param target_info:
    :param asset_id:
    :param partition_instance:
    :param location:
    :return:
    """
    partition_location = location.replace("s3a", "s3")
    ath = boto3.client('athena', region_name=region)
    db = target_info['domain']
    table = target_info['subdomain'] + "_" + asset_id
    # Alter table statement
    alter_table = f"""
    ALTER TABLE {db}.{table} ADD IF NOT EXISTS 
    PARTITION (partition_instance='{partition_instance}')
    LOCATION '{partition_location}';
    """
    # Execute the partition statement on Athena (Workgroup Hardcoded)
    ath.start_query_execution(QueryString=alter_table, WorkGroup='dl-fmwrk')
