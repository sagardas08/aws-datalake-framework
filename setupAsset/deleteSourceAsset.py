# usage: python setupAsset/deleteSourceAsset.py asset_id region

import sys
import json
import boto3
from connector.pg_connect import Connector
from utils.comUtils import getGlobalParams
from utils.logger import Logger

# Declaring Logger and Config
logger = Logger()
config_file_path = "config/globalConfig.json"
file = open(file=config_file_path, mode="r")
config = json.load(file)
file.close()


class Asset:
    def __init__(self, db, asset_id, region):
        """
        A reusable class to contain the associated asset information
        :param db: postgres database connector
        :param asset_id: Asset ID
        :param region: Region e.g. us-east-1
        """
        self.asset_id = asset_id
        self.region = region
        self.fm_prefix = config["fm_prefix"]
        # TODO: DynamoDB -> RDS: client Library

        # self.connector = Connector(global_config["db_secret"], global_config["db_region"], autocommit=True)
        self.db = db
        self.asset_details = self.get_asset_details(db)
        self.src_sys_id = self.asset_details["src_sys_id"]

    def get_asset_details(self, db):
        # TODO: DynamoDB -> RDS: Retrieve Data
        response = db.retrieve_dict(table="data_asset", cols="all", where=("asset_id = %s", [self.asset_id]))
        return response[0]


def remove_asset_s3(asset):
    """
    Remove asset details from the stored S3 folder
    :param asset:
    :return: None
    """
    s3_client = boto3.client("s3", region_name=asset.region)
    # Creating the bucket string
    bucket = f"{asset.fm_prefix}-{asset.src_sys_id}-{asset.region}"
    objects = s3_client.list_objects(
        Bucket=bucket, Prefix=str(asset.asset_id)
    )

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
    config_ob = s3_client.get_bucket_notification_configuration(
        Bucket=bucket
    )
    current_bucket_config = config_ob["TopicConfigurations"]
    updated_bucket_config = [
        i
        for i in current_bucket_config
        if str(asset.asset_id) not in i["Id"]
    ]
    s3 = boto3.resource("s3")
    bucket_notification = s3.BucketNotification(bucket)
    try:
        bucket_notification.put(
            NotificationConfiguration={
                "TopicConfigurations": updated_bucket_config
            }
        )
    except Exception as e:
        logger.write(level=50, message=str(e))


def remove_asset_col_details(asset):
    # TODO: DynamoDB -> RDS: Delete Table
    # Delete details of this asset from dl_fmwrk.data_asset_attributes
    """
    Remove the table fm_prefix.data_asset.asset_id from dynamodb
    """
    try:
        asset.db.delete(table="data_asset_attributes", where=("asset_id = %s", [asset.asset_id]))
        logger.write(
            message=f"Deleted records of asset_id: {asset.asset_id} from data_asset_attributes"
        )
    except Exception as error:
        logger.write(message=f"Exception Type: {type(error)}")


def remove_asset_catalog_details(asset):
    # TODO: DynamoDB -> RDS: Delete Table
    # delete asset from dl_fmwrk.data_asset_catalogs
    """
    Remove the table fm_prefix.data_catalog.asset_id from dynamodb
    :param asset:
    :return:
    """
    try:
        asset.db.delete(table="data_asset_catalogs", where=("asset_id = %s", [asset.asset_id]))
        logger.write(
            message=f"Deleted records of asset_id: {asset.asset_id} from data_asset_catalog"
        )
    except Exception as error:
        logger.write(message=f"Exception Type: {type(error)}")


def remove_asset_item(asset):
    # TODO: DynamoDB -> RDS: Delete an entry from a Table
    """
    Remove Asset details from fm_prefix.data_asset
    """
    try:
        asset.db.delete(table="data_asset", where=("asset_id = %s", [asset.asset_id]))
        logger.write(
            message=f"Deleted {asset.asset_id} from data_asset"
        )
    except Exception as error:
        logger.write(message=f"Exception Type: {type(error)}")


def delete_source_asset_region_wise(asset):
    """
    Remove the asset in the specified region
    """
    remove_asset_s3(asset)
    remove_asset_col_details(asset)
    remove_asset_catalog_details(asset)
    remove_asset_item(asset)


def delete_source_asset(db, asset_id, region=None):
    """
    Remove the details of an asset from the source system
    :param db: postgres database connector
    :param asset_id: Asset ID
    :param region: Region e.g. us-east-1
    :return: None
    """
    if region:
        # if a region is specified then delete the asset in that region
        asset = Asset(db, asset_id, region)
        delete_source_asset_region_wise(asset)
    else:
        # Remove the asset in all the regions
        regions = [config["primary_region"], config["secondary_region"]]
        for region in regions:
            asset = Asset(db, asset_id, region)
            delete_source_asset_region_wise(asset)


def main():
    """
    Main entry point of the module
    """
    global_config = getGlobalParams()
    arguments = sys.argv
    db = Connector(global_config["db_secret"], global_config["db_region"], autocommit=True)
    try:
        if len(arguments) < 2:
            logger.write(message="Missing Arguments: asset_id and region")
        elif len(arguments) == 2:
            asset_id = arguments[1]
            logger.write(message=f"Attempting to delete asset: {asset_id}")
            delete_source_asset(db, asset_id)
        elif len(arguments) == 3:
            asset_id = arguments[1]
            region = arguments[2]
            logger.write(
                message=f"Attempting to delete asset: {asset_id} in region: {region}"
            )
            delete_source_asset(db, asset_id, region)
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    main()
