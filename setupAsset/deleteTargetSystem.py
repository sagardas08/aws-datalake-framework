import json
import boto3
import sys

from connector import Connector
from utils.logger import Logger

# Create logger object
logger = Logger()
config_file_path = "config/globalConfig.json"
file = open(file=config_file_path, mode="r")
global_config = json.load(file)
print(global_config)


def move_tgt_sys_to_archive(tgt_sys_id, region):
    """
    Move the files in the target bucket to archive folder in the same bucket
    :param tgt_sys_id: The target system ID
    :param region: Region where the target bucket is present
    :return: None
    """
    client = boto3.client("s3")
    s3 = boto3.resource("s3")
    bucket_name = global_config["fm_tgt_prefix"] + "-" + str(tgt_sys_id) + "-" + region
    try:
        my_bucket = s3.Bucket(bucket_name)
        # Move all the folders in the target bucket into archive folder
        for key_file in my_bucket.objects.all():
            if key_file.key.split("/")[0] != "archive":
                k = key_file.key
                copy_source = {"Bucket": bucket_name, "Key": k}
                client.copy_object(
                    Bucket=bucket_name, CopySource=copy_source, Key="archive/" + k
                )
        # Delete all the folders in the target bucket except the archive folder
        for obj in my_bucket.objects.all():
            if obj.key.split("/")[0] != "archive":
                s3.Object(my_bucket.name, obj.key).delete()
    except Exception as e:
        logger.write(message=str(e))


def del_tgt_sys_item(db, tgt_sys_id):
    """
    Delete the target system item from postgres table
    :param tgt_sys_id: Target System ID
    :param region: Region where the dynamoDb table is present
    :return: None
    """
    try:
        table = "target_system"
        condition = ('target_id = %s', [tgt_sys_id])
        db.delete(table, condition)
    except Exception as e:
        logger.write(message=str(e))


def check_if_associated_with_asset(db, target_id):
    """
    Check if target system is associated with asset
    :param target_id: Target system ID
    :param region: Region where dynamoDB table is present
    :return: Boolean
    """
    try:
        # check the data asset table to see
        # if the target sys is attached to any asset
        table = 'data_asset'
        condition = ('target_id = %s', [target_id])
        response = db.retrieve_dict(table, cols='*', where=condition)
        if response:
            return True
        else:
            return False
    except Exception as e:
        logger.write(message=str(e))


def check_if_tgt_sys_present_or_not(db, target_id):
    """
    Check if target system is present or not
    :param target_id:Target system ID
    :return:Boolean
    """
    try:
        table = "target_system"
        condition = ('target_id = %s', [target_id])
        # Trying to get postgres item with src_sys_id and bucket name as key
        response = db.retrieve_dict(
            table, cols='target_id', where=condition
        )
        # If item with the specified tgt_sys_id is present,the response contains "Item" in it
        if response:
            # Returns True if tgt_sys_id is present
            return True
        else:
            # Returns False if tgt_sys_id is absent
            return False
    except Exception as e:
        logger.write(message=str(e))


def del_tgt_system_with_region(db, target_id, region):
    """
    Delete the target system region wise
    :param target_id: Target system ID
    :param region: Region
    :return:None
    """
    # Condition to check if target system is present before deletion
    if check_if_tgt_sys_present_or_not(db, target_id):
        # Check if target system associated with asset
        associated_or_not = check_if_associated_with_asset(db, target_id)
        # If it is not associated,target system will be deleted
        if not associated_or_not:
            del_tgt_sys_item(db, target_id)
            move_tgt_sys_to_archive(target_id, region)
            # If it is associated,target system will not be deleted as the data asset need to be deleted first
        elif associated_or_not:
            logger.write(
                message=f"Target system {target_id} cannot be deleted.Data asset associated with it"
            )
    else:
        # If target system is not present
        logger.write(message=f"Target system {target_id} does not exist")


def del_tgt_system(db, tgt_sys_id, region=None):
    """
    Delete the target system
    :param tgt_sys_id: Target system ID
    :param region: Region
    :return: None
    """
    if region is None:
        primary_region = global_config["primary_region"]
        secondary_region = global_config["secondary_region"]
        # If region is not specified,deletion will take place in both the regions
        del_tgt_system_with_region(db, tgt_sys_id, primary_region)
        del_tgt_system_with_region(db, tgt_sys_id, secondary_region)
    else:
        # If region is specified,deletion will take place in only the specified region
        del_tgt_system_with_region(db, tgt_sys_id, region)


def main():
    """
    :return:None
    """
    arguments = sys.argv
    arguments = sys.argv
    db_secret = global_config['db_secret']
    db_region = global_config['db_region']
    db = Connector(
        secret=db_secret, region=db_region, autocommit=True
    )
    # If no argument is passed as command line argument
    if len(arguments) < 2:
        logger.write(message="Missing Arguments: tgt_sys_id and region")
    # If only src_sys_id is passed as command line argument
    elif len(arguments) == 2:
        tgt_sys_id = arguments[1]
        logger.write(message=f"Attempting to delete target_system: {tgt_sys_id}")
        del_tgt_system(db, tgt_sys_id)
    # If both src_sys_id and region are passed as command line arguments
    elif len(arguments) == 3:
        tgt_sys_id = arguments[1]
        region = arguments[2]
        logger.write(
            message=f"Attempting to delete target_system: {tgt_sys_id} in region: {region}"
        )
        del_tgt_system(db, tgt_sys_id, region)


if __name__ == "__main__":
    main()
