import json
import boto3


def create_source_system_table(prefix, region=None):
    # TODO: DynamoDB -> RDS: Create Table
    """
    Creates dynamodb table for source system
    """
    dynamodb = boto3.client("dynamodb", region_name=region)
    existing_tables = dynamodb.list_tables()["TableNames"]
    table_name = f"{prefix}.source_system"
    if table_name not in existing_tables:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "src_sys_id", "KeyType": "HASH"},
                {"AttributeName": "bucket_name", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "src_sys_id", "AttributeType": "N"},
                {"AttributeName": "bucket_name", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 100, "WriteCapacityUnits": 100},
        )


def create_data_asset_table(prefix, region):
    # TODO: DynamoDB -> RDS: Create Table
    """
    Creates dynamodb table for data asset
    """
    dynamodb = boto3.client("dynamodb", region_name=region)
    existing_tables = dynamodb.list_tables()["TableNames"]
    table_name = f"{prefix}.data_asset"
    if table_name not in existing_tables:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "asset_id", "KeyType": "HASH"},
                {"AttributeName": "src_sys_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "asset_id", "AttributeType": "N"},
                {"AttributeName": "src_sys_id", "AttributeType": "N"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 100, "WriteCapacityUnits": 100},
        )


def create_target_system_table(prefix, region):
    # TODO: DynamoDB -> RDS: Create Table
    """
    Creates dynamodb table for target system
    """
    dynamodb = boto3.client("dynamodb", region_name=region)
    existing_tables = dynamodb.list_tables()["TableNames"]
    table_name = f"{prefix}.target_system"
    if table_name not in existing_tables:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "tgt_sys_id", "KeyType": "HASH"},
                {"AttributeName": "bucket_name", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "tgt_sys_id", "AttributeType": "N"},
                {"AttributeName": "bucket_name", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 100, "WriteCapacityUnits": 100},
        )


def create_dynamodb_tables(config, region=None):
    region = config["primary_region"] if region is None else region
    prefix = config["fm_prefix"]
    try:
        create_source_system_table(prefix, region)
        create_data_asset_table(prefix, region)
        create_target_system_table(prefix, region)
        return True
    except Exception as e:
        print(e)
        return False
