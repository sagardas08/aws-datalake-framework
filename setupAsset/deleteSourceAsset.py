# usage: python setupAsset/deleteSourceAsset.py asset_id region

import json
import sys

import boto3
from boto3.dynamodb.conditions import Key
from botocore.errorfactory import ClientError

from utils.logger import Logger

# Declaring Logger and Config
logger = Logger()
config_file_path = "config/globalConfig.json"
file = open(file=config_file_path, mode="r")
config = json.load(file)
file.close()


class Asset:
    def __init__(self, asset_id, region):
        """
        A reusable class to contain the associated asset information
        :param asset_id:
        :param region:
        """
        self.asset_id = asset_id
        self.region = region
        self.fm_prefix = config["fm_prefix"]
        self.dynamodb = boto3.resource("dynamodb", region_name=self.region)
        self.dynamodb_client = boto3.client("dynamodb", region_name=self.region)
        self.asset_details = self.get_asset_details()
        self.src_sys_id = self.asset_details["src_sys_id"]

    def get_asset_details(self):
        table = self.dynamodb.Table(f"{self.fm_prefix}.data_asset")
        response = table.query(
            KeyConditionExpression=Key("asset_id").eq(int(self.asset_id))
        )
        return response["Items"][0]


def remove_asset_s3(asset):
    """
    Remove asset details from the stored S3 folder
    :param asset:
    :return:
    """
    s3_client = boto3.client("s3", region_name=asset.region)
    # Creating the bucket string
    bucket = f"{asset.fm_prefix}-{asset.src_sys_id}-{asset.region}"
    objects = s3_client.list_objects(Bucket=bucket, Prefix=asset.asset_id)

    try:
        # Listing the files present in the bucket associated with the asset id
        for key in objects["Contents"]:
            file_key = key["Key"]
            logger.write(message=f"Deleting {file_key}")
            s3_client.delete_object(Bucket=bucket, Key=file_key)
        s3_client.delete_object(Bucket=bucket, Key=f"{asset.asset_id}")
    except KeyError:
        # If a particular asset files are not present then
        logger.write(level=50, message="No files found to delete.")

    # Remove the associated event notification rule
    config_ob = s3_client.get_bucket_notification_configuration(Bucket=bucket)
    current_bucket_config = config_ob["TopicConfigurations"]
    updated_bucket_config = [
        i for i in current_bucket_config if str(asset.asset_id) not in i["Id"]
    ]
    s3 = boto3.resource("s3")
    bucket_notification = s3.BucketNotification(bucket)
    try:
        bucket_notification.put(
            NotificationConfiguration={"TopicConfigurations": updated_bucket_config}
        )
    except Exception as e:
        logger.write(level=50, message=str(e))


def remove_asset_col_details(asset):
    """
    Remove the table fm_prefix.data_asset.asset_id from dynamodb
    :param asset:
    :return:
    """
    asset_detail_table = f"{asset.fm_prefix}.data_asset.{asset.asset_id}"
    try:
        asset.dynamodb_client.delete_table(TableName=asset_detail_table)
        logger.write(message=f"Deleted Table {asset_detail_table} from DynamoDB")
    except ClientError:
        logger.write(message=f"The table: {asset_detail_table} d.n.e.")


def remove_asset_catalog_details(asset):
    """
    Remove the table fm_prefix.data_catalog.asset_id from dynamodb
    :param asset:
    :return:
    """
    asset_catalog_table = f"{asset.fm_prefix}.data_catalog.{asset.asset_id}"
    try:
        asset.dynamodb_client.delete_table(TableName=asset_catalog_table)
        logger.write(message=f"Deleted Table {asset_catalog_table} from DynamoDB")
    except ClientError:
        logger.write(message=f"The table: {asset_catalog_table} d.n.e.")


def remove_asset_item(asset):
    """
    Remove Asset details from fm_prefix.data_asset
    :param asset:
    :return:
    """
    asset_detail_table = f"{asset.fm_prefix}.data_asset"
    table = asset.dynamodb.Table(asset_detail_table)
    try:
        response = table.delete_item(
            Key={"asset_id": int(asset.asset_id), "src_sys_id": int(asset.src_sys_id)}
        )
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if int(status_code) == 200:
            logger.write(message=f"Deleted {asset.asset_id} from {asset_detail_table}")
        else:
            logger.write(
                message=f"Unable to delete {asset.asset_id} from {asset_detail_table}"
            )
    except ClientError:
        logger.write(message=f"The table: {asset_detail_table} d.n.e.")


def delete_source_asset_region_wise(asset):
    """
    Remove the asset in the specified region
    :param asset:
    :return:
    """
    #
    remove_asset_s3(asset)
    remove_asset_item(asset)
    remove_asset_col_details(asset)
    remove_asset_catalog_details(asset)


def delete_source_asset(asset_id, region=None):
    """
    Remove the details of an asset from the source system
    :param asset_id:
    :param region:
    :return:
    """
    if region:
        asset = Asset(asset_id, region)
        delete_source_asset_region_wise(asset)
    else:
        # Remove the asset in all the regions
        regions = [config["primary_region"], config["secondary_region"]]
        for region in regions:
            asset = Asset(asset_id, region)
            delete_source_asset_region_wise(asset)


def main():
    """

    :return:
    """
    arguments = sys.argv
    if len(arguments) < 2:
        logger.write(message="Missing Arguments: asset_id and region")
    elif len(arguments) == 2:
        asset_id = arguments[1]
        logger.write(message=f"Attempting to delete asset: {asset_id}")
        delete_source_asset(asset_id)
    elif len(arguments) == 3:
        asset_id = arguments[1]
        region = arguments[2]
        logger.write(
            message=f"Attempting to delete asset: {asset_id} in region: {region}"
        )
        delete_source_asset(asset_id, region)


if __name__ == "__main__":
    main()
