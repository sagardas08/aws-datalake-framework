import json
import boto3
import sys
from boto3.dynamodb.conditions import Key
from utils.logger import Logger

# Create logger object
logger = Logger()


def get_global_params():
    """
    Utility method to get global config file
    :return:JSON
    """
    config_file_path = "config/globalConfig.json"
    file = open(file=config_file_path, mode="r")
    global_config = json.load(file)
    return global_config


def move_tgt_sys_to_archive(tgt_sys_id, region):
    """
    Move the files in the target bucket to archive folder in the same bucket
    :param tgt_sys_id: The target system ID
    :param region: Region where the target bucket is present
    :return: None
    """
    global_config = get_global_params()
    client = boto3.client("s3")
    s3 = boto3.resource("s3")
    bucket_name = global_config["fm_tgt_prefix"] + "-" + str(tgt_sys_id) + "-" + region
    my_bucket = s3.Bucket(bucket_name)
    try:
        # Move all the folders in the target bucket into archive folder
        for file in my_bucket.objects.all():
            if file.key.split("/")[0] != "archive":
                k = file.key
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


def del_tgt_sys_item(tgt_sys_id, region):
    """
    Delete the target system item from dynamodb table
    :param tgt_sys_id: Target System ID
    :param region: Region where the dynamoDb table is present
    :return: None
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    try:
        global_config = get_global_params()
        fm_prefix = global_config["fm_prefix"]
        table = dynamodb.Table(f"{fm_prefix}.target_system")
        bucket_name = (
            global_config["fm_tgt_prefix"] + "-" + str(tgt_sys_id) + "-" + region
        )
        # Delete item with tgt_sys_id and bucket name as key
        table.delete_item(
            Key={"tgt_sys_id": int(tgt_sys_id), "bucket_name": bucket_name}
        )
    except Exception as e:
        logger.write(message=str(e))


def check_if_associated_with_asset(tgt_sys_id, region):
    """
    Check if target system is associated with asset
    :param tgt_sys_id: Target system ID
    :param region: Region where dynamoDB table is present
    :return: Boolean
    """
    global_config = get_global_params()
    fm_prefix = global_config["fm_prefix"]
    client = boto3.client("dynamodb", region_name=region)
    try:
        # Access the data asset dynamoDb table
        response = client.scan(TableName=f"{fm_prefix}.data_asset")
        # TODO:
        # Loop over to find out if the target ID is present in the data asset table or not.
        for i in response["Items"]:
            for k, v in i["target_id"].items():
                if str(v) == str(tgt_sys_id):
                    return True
        return False
    except Exception as e:
        logger.write(message=str(e))


def check_if_tgt_sys_present_or_not(tgt_sys_id, region):
    """
    Check if target system is present or not
    :param tgt_sys_id:Target system ID
    :param region: Region where dynamoDb table is present
    :return:Boolean
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    try:
        global_config = get_global_params()
        fm_prefix = global_config["fm_prefix"]
        table = dynamodb.Table(f"{fm_prefix}.target_system")
        bucket_name = (
            global_config["fm_tgt_prefix"] + "-" + str(tgt_sys_id) + "-" + region
        )
        # Trying to get dynamoDB item with tgt_sys_id and bucket name as key
        response = table.get_item(
            Key={"tgt_sys_id": int(tgt_sys_id), "bucket_name": bucket_name}
        )
        # If item with the specified tgt_sys_id is present,the response contains "Item" in it
        if "Item" in response:
            # Returns True if tgt_sys_id is present
            return True
        else:
            # Returns False if tgt_sys_id is absent
            return False
    except Exception as e:
        logger.write(message=str(e))


def del_tgt_system_with_region(tgt_sys_id, region):
    """
    Delete the target system region wise
    :param tgt_sys_id: Target system ID
    :param region: Region
    :return:None
    """
    # Condition to check if target system is present before deletion
    if check_if_tgt_sys_present_or_not(tgt_sys_id, region) == True:
        # Check if target system associated with asset
        associated_or_not = check_if_associated_with_asset(tgt_sys_id, region)
        # If it is not associated,target system will be deleted
        if associated_or_not == False:
            del_tgt_sys_item(tgt_sys_id, region)
            move_tgt_sys_to_archive(tgt_sys_id, region)
            # If it is associated,target system will not be deleted as the data asset need to be deleted first
        elif associated_or_not == True:
            logger.write(
                message=f"Target system {tgt_sys_id} cannot be deleted.Data asset associated with it"
            )
    else:
        # If target system is not present
        logger.write(message=f"Target system {tgt_sys_id} does not exist")


def del_tgt_system(tgt_sys_id, region=None):
    """
    Delete the target system
    :param tgt_sys_id: Target system ID
    :param region: Region
    :return: None
    """
    if region == None:
        global_config = get_global_params()
        primary_region = global_config["primary_region"]
        secondary_region = global_config["secondary_region"]
        # If region is not specified,deletion will take place in both the regions
        del_tgt_system_with_region(tgt_sys_id, primary_region)
        del_tgt_system_with_region(tgt_sys_id, secondary_region)
    else:
        # If region is specified,deletion will take place in only the specified region
        del_tgt_system_with_region(tgt_sys_id, region)


def main():
    """
    :return:None
    """
    arguments = sys.argv
    # If no argument is passed as command line argument
    if len(arguments) < 2:
        logger.write(message="Missing Arguments: tgt_sys_id and region")
    # If only src_sys_id is passed as command line argument
    elif len(arguments) == 2:
        tgt_sys_id = arguments[1]
        logger.write(message=f"Attempting to delete target_system: {tgt_sys_id}")
        del_tgt_system(tgt_sys_id)
    # If both src_sys_id and region are passed as command line arguments
    elif len(arguments) == 3:
        tgt_sys_id = arguments[1]
        region = arguments[2]
        logger.write(
            message=f"Attempting to delete target_system: {tgt_sys_id} in region: {region}"
        )
        del_tgt_system(tgt_sys_id, region)


if __name__ == "__main__":
    main()
