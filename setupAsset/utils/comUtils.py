import json
import decimal
import time
import sys
import os
import random
import boto3


def getGlobalParams():
    script_dir = os.path.dirname(
        __file__
    )  # <-- absolute dir the script is in
    gbl_cfg_rel_path = "../config/globalConfig.json"
    gbl_cfg_abs_path = os.path.join(script_dir, gbl_cfg_rel_path)

    with open(gbl_cfg_abs_path) as json_file:
        global_config = json.load(json_file)
        return global_config


def insert_asset_item_dynamoDB(asset_json_file, asset_id, region):
    # TODO: DynamoDB -> RDS: Insert Data
    dynamodb = boto3.resource("dynamodb", region_name=region)
    global_config = getGlobalParams()

    with open(asset_json_file) as json_file:
        asset_config = json.load(json_file)

    asset_config.update({"asset_id": asset_id})
    item = json.dumps(asset_config)

    asset_table = dynamodb.Table(
        "{}.data_asset".format(global_config["fm_prefix"])
    )
    jsonItem = json.loads(item)

    print(
        "Inserting {} info in {}.data_asset table in {}".format(
            asset_id, global_config["fm_prefix"], region
        )
    )
    response = asset_table.put_item(Item=jsonItem)


def insert_asset_cols_dynamoDB(asset_col_json_file, asset_id, region):
    # TODO: DynamoDB -> RDS: Insert Data
    dynamodb = boto3.resource("dynamodb", region_name=region)
    global_config = getGlobalParams()

    with open(asset_col_json_file) as json_file:
        asset_col_config = json.load(json_file)

    print(
        "Inserting column info in {}.data_asset.{} table in {}".format(
            global_config["fm_prefix"], asset_id, region
        )
    )
    for rows in asset_col_config["columns"]:
        item = json.dumps(rows)
        jsonItem = json.loads(item)
        asset_col_table = dynamodb.Table(
            "{}.data_asset.{}".format(
                global_config["fm_prefix"], asset_id
            )
        )
        response = asset_col_table.put_item(Item=jsonItem)


def create_asset_catalog_table(asset_id, region):
    # TODO: DynamoDB -> RDS: Create Table
    dynamodb = boto3.resource("dynamodb", region_name=region)
    global_config = getGlobalParams()

    print(
        "Creating the table {}.data_catalog.{} in {}".format(
            global_config["fm_prefix"], str(asset_id), region
        )
    )
    asset_detail_table = dynamodb.create_table(
        TableName=global_config["fm_prefix"]
        + ".data_catalog."
        + str(asset_id),
        KeySchema=[
            {"AttributeName": "exec_id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "exec_id", "AttributeType": "S"},
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1,
        },
    )


def create_asset_detail_table(asset_id, region):
    # TODO: DynamoDB -> RDS: Create Table
    dynamodb = boto3.resource("dynamodb", region_name=region)
    global_config = getGlobalParams()

    print(
        "Creating the table {}.data_asset.{} in {}".format(
            global_config["fm_prefix"], str(asset_id), region
        )
    )
    asset_detail_table = dynamodb.create_table(
        TableName=global_config["fm_prefix"]
        + ".data_asset."
        + str(asset_id),
        KeySchema=[
            {"AttributeName": "col_id", "KeyType": "HASH"},
            {"AttributeName": "col_nm", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "col_id", "AttributeType": "N"},
            {"AttributeName": "col_nm", "AttributeType": "S"},
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1,
        },
    )


def create_src_s3_dir_str(asset_id, asset_json_file, region):
    global_config = getGlobalParams()
    with open(asset_json_file) as json_file:
        asset_config = json.load(json_file)

    src_sys_id = asset_config["src_sys_id"]
    bucket_name = (
        global_config["fm_prefix"]
        + "-"
        + str(src_sys_id)
        + "-"
        + region
    )

    print(
        "Creating directory structure in {} bucket".format(bucket_name)
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/init/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/error/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/masked/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/error/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/logs/dummy"'.format(
            bucket_name, asset_id
        )
    )


def set_bucket_event_notification(asset_id, asset_json_file, region):
    """
    Utility method to set the bucket event notification by getting prior event settings

    :param asset_id:
    :param asset_json_file: A json formatted document that contains details about the asset
    :param region:

    :return:
    """
    global_config = getGlobalParams()
    with open(asset_json_file) as json_file:
        asset_config = json.load(json_file)

    src_sys_id = asset_config["src_sys_id"]
    bucket_name = (
        global_config["fm_prefix"]
        + "-"
        + str(src_sys_id)
        + "-"
        + region
    )
    key_prefix = str(asset_id) + "/init/"
    if not asset_config["multipartition"]:
        key_suffix = asset_config["file_type"]
    else:
        key_suffix = asset_config["trigger_file_pattern"]
    s3_event_name = str(asset_id) + "-createObject"
    sns_name = (
        global_config["fm_prefix"]
        + "-"
        + str(src_sys_id)
        + "-init-file-creation"
    )
    sns_arn = (
        "arn:aws:sns:"
        + region
        + ":"
        + global_config["aws_account"]
        + ":"
        + sns_name
    )
    s3Client = boto3.client("s3")
    print(
        "Creating putObject event notification to {} bucket".format(
            bucket_name
        )
    )
    new_config = {
        "Id": s3_event_name,
        "TopicArn": sns_arn,
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
            "Key": {
                "FilterRules": [
                    {"Name": "prefix", "Value": key_prefix},
                    {"Name": "suffix", "Value": key_suffix},
                ]
            }
        },
    }
    response = s3Client.get_bucket_notification_configuration(
        Bucket=bucket_name
    )
    if "TopicConfigurations" not in response.keys():
        # If the bucket doesn't have event notification configured
        # Attach a new Topic Config
        s3Client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "TopicConfigurations": [new_config]
            },
        )
    else:
        # The bucket has event notif configured.
        # Get the initial configs and append the new config
        bucket_config = response["TopicConfigurations"]
        bucket_config.append(new_config)
        s3Client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "TopicConfigurations": bucket_config
            },
        )
