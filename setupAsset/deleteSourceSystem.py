import boto3
import json
import sys
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


def delete_src_sys_stack(src_sys_id, region):
    """
    Delete the source system stack
    :param src_sys_id: Source system ID
    :param region: Region where the stack is present
    :return: None
    """
    global_config = get_global_params()
    stack_name = global_config["fm_prefix"] + "-" + str(src_sys_id) + "-" + region
    client = boto3.client("cloudformation", region_name=region)
    try:
        # Deletion of stack
        client.delete_stack(StackName=stack_name)
    except Exception as e:
        logger.write(message=str(e))


def check_if_associated_with_asset(src_sys_id, region):
    """
    Check if the source system is associated with asset in the dynamoDB table
    :param src_sys_id: Source system ID
    :param region: Region
    :return: True or False
    """
    global_config = get_global_params()
    fm_prefix = global_config["fm_prefix"]
    client = boto3.client("dynamodb", region_name=region)
    try:
        # Accessing data asset table
        response = client.scan(TableName=f"{fm_prefix}.data_asset")
        # Looping over the items to check if source system is associated with any asset
        # TODO:
        for i in response["Items"]:
            for k, v in i["src_sys_id"].items():
                if str(v) == str(src_sys_id):
                    return True
        return False
    except Exception as e:
        logger.write(message=str(e))


def check_if_src_sys_present_or_not(src_sys_id, region):
    """
    Check if source system is present or not
    :param src_sys_id: Source system ID
    :param region: Region
    :return: Boolean
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    try:
        global_config = get_global_params()
        fm_prefix = global_config["fm_prefix"]
        table = dynamodb.Table(f"{fm_prefix}.source_system")
        bucket_name = global_config["fm_prefix"] + "-" + str(src_sys_id) + "-" + region
        # Trying to get dynamoDB item with src_sys_id and bucket name as key
        response = table.get_item(
            Key={"src_sys_id": int(src_sys_id), "bucket_name": bucket_name}
        )
        # If item with the specified src_sys_id is present,the response contains "Item" in it
        if "Item" in response:
            # Returns True if src_sys_id is present
            return True
        else:
            # Returns False if src_sys_id is absent
            return False
    except Exception as e:
        logger.write(message=str(e))


def del_src_sys_with_region(src_sys_id, region):
    """
    Delete the source system stack region wise
    :param src_sys_id:Source system ID
    :param region: Region
    :return:None
    """
    # Condition to check if source system is present before deletion
    if check_if_src_sys_present_or_not(src_sys_id, region) == True:
        # Check if source system associated with asset
        associated_or_not = check_if_associated_with_asset(src_sys_id, region)
        # If it is not associated,source system stack will be deleted
        if associated_or_not == False:
            delete_src_sys_stack(src_sys_id, region)
        # If it is associated,source system stack will not be deleted as the data asset need to be deleted first
        elif associated_or_not == True:
            logger.write(
                message=f"Source system {src_sys_id} cannot be deleted.Data asset associated with it"
            )
    else:
        # If source system is not present
        logger.write(message=f"Source system {src_sys_id} does not exist")


def del_src_system(src_sys_id, region=None):
    """
    Delete the source system stack
    :param src_sys_id:Source system ID
    :param region: Region
    :return: None
    """
    if region == None:
        global_config = get_global_params()
        primary_region = global_config["primary_region"]
        secondary_region = global_config["secondary_region"]
        # If region is not specified,deletion will take place in both the regions
        del_src_sys_with_region(src_sys_id, primary_region)
        del_src_sys_with_region(src_sys_id, secondary_region)
    else:
        # If region is specified,deletion will take place in only the specified region
        del_src_sys_with_region(src_sys_id, region)


def main():
    """
    :return:None
    """
    arguments = sys.argv
    # If no argument is passed as command line argument
    if len(arguments) < 2:
        logger.write(message="Missing Arguments: src_sys_id and region")
    # If only src_sys_id is passed as command line argument
    elif len(arguments) == 2:
        src_sys_id = arguments[1]
        logger.write(message=f"Attempting to delete source_system: {src_sys_id}")
        del_src_system(src_sys_id)
    # If both src_sys_id and region are passed as command line arguments
    elif len(arguments) == 3:
        src_sys_id = arguments[1]
        region = arguments[2]
        logger.write(
            message=f"Attempting to delete source_system: {src_sys_id} in region: {region}"
        )
        del_src_system(src_sys_id, region)


if __name__ == "__main__":
    main()
