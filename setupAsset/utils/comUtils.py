import json
import os

import boto3

from connector.pg_connect import Connector


def getGlobalParams():
    # absolute dir the script is in
    script_dir = os.path.dirname(__file__)
    gbl_cfg_rel_path = "../config/globalConfig.json"
    gbl_cfg_abs_path = os.path.join(script_dir, gbl_cfg_rel_path)
    with open(gbl_cfg_abs_path) as json_file:
        json_config = json.load(json_file)
        return json_config


global_config = getGlobalParams()


def insert_asset_item_rds(db, asset_json_file, asset_id, region):
    """

    :param db:
    :param asset_json_file:
    :param asset_id:
    :param region:
    :return:
    """
    with open(asset_json_file) as json_file:
        asset_config = json.load(json_file)
    asset_config.update({"asset_id": asset_id})
    item = json.dumps(asset_config)
    jsonItem = json.loads(item)
    fm_prefix = global_config['fm_prefix']
    table = "data_asset"
    print(
        f"Inserting {asset_id} info in table in {fm_prefix}.{table}"
    )
    db.insert(table=table, data=jsonItem)


def insert_asset_cols_rds(db, asset_col_json_file, asset_id, region):
    """

    :param db:
    :param asset_col_json_file:
    :param asset_id:
    :param region:
    :return:
    """
    with open(asset_col_json_file) as json_file:
        asset_col_config = json.load(json_file)
    fm_prefix = global_config['fm_prefix']
    table = "data_asset_attributes"
    print(
        "Inserting column info for asset: {} in "
        "{}.data_asset_attributes table in region {}".format(
            asset_id, fm_prefix, region
        )
    )
    data = asset_col_config["columns"]
    if data[0]['asset_id'] != asset_id:
        for i in data:
            i['asset_id'] = asset_id
    db.insert_many(table, data)


def create_src_s3_dir_str(asset_id, asset_json_file, region):
    # global_config = getGlobalParams()
    with open(asset_json_file) as json_file:
        asset_config = json.load(json_file)

    src_sys_id = asset_config["src_sys_id"]
    # bucket_name = global_config["fm_prefix"] + "-" + str(src_sys_id) + "-"+ region
    bucket_name = f"{global_config['fm_prefix']}-{str(src_sys_id)}-{region}"
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

