import boto3
import json
import sys
from utils.logger import Logger
from connector import Connector

# Create logger object
logger = Logger()
config_file_path = "config/globalConfig.json"
file = open(file=config_file_path, mode="r")
global_config = json.load(file)


def delete_src_sys_stack(src_sys_id, region):
    """
    Delete the source system stack
    :param src_sys_id: Source system ID
    :param region: Region where the stack is present
    :return: None
    """
    stack_name = global_config["fm_prefix"] + "-" + str(src_sys_id) + "-" + region
    client = boto3.client("cloudformation", region_name=region)
    try:
        # Deletion of stack
        client.delete_stack(StackName=stack_name)
    except Exception as e:
        logger.write(message=str(e))


def is_associated_with_asset(db, src_sys_id, region):
    """
    Check if the source system is associated with asset in the dynamoDB table
    :param src_sys_id: Source system ID
    :param region: Region
    :return: True or False
    """
    fm_prefix = global_config["fm_prefix"]
    client = boto3.client("dynamodb", region_name=region)
    try:
        # Accessing data asset table
        table = 'data_asset'
        condition = ('src_sys_id = %s', [src_sys_id])
        # Trying to get dynamoDB item with src_sys_id and bucket name as key
        response = db.retrieve_dict(
            table, cols='src_sys_id', where=condition
        )
        if response:
            return True
        else:
            return False
    except Exception as e:
        logger.write(message=str(e))


def src_sys_present(db, src_sys_id, region):
    """
    Check if source system is present or not
    :param src_sys_id: Source system ID
    :param region: Region
    :return: Boolean
    """
    try:
        table = 'source_system'
        condition = ('src_sys_id = %s', [src_sys_id])
        # Trying to get dynamoDB item with src_sys_id and bucket name as key
        response = db.retrieve_dict(
            table, cols='src_sys_id', where=condition
        )
        # If item with the specified src_sys_id is present,the response contains "Item" in it
        if response:
            # Returns True if src_sys_id is present
            return True
        else:
            # Returns False if src_sys_id is absent
            return False
    except Exception as e:
        logger.write(message=str(e))


def del_source_sys_item(db, src_sys_id):
    """
    Delete the source system item from postgres table
    :param src_sys_id: Target System ID
    :return: None
    """
    try:
        src_sys_id_int = int(src_sys_id)
        table = "source_system"
        condition = ('src_sys_id = %s', [src_sys_id_int])
        db.delete(table, condition)
    except Exception as e:
        logger.write(message=str(e))


def del_src_sys_with_region(db, src_sys_id, region):
    """
    Delete the source system stack region wise
    :param db: postgres db connector
    :param src_sys_id:Source system ID
    :param region: Region
    :return:None
    """
    # Condition to check if source system is present before deletion
    if src_sys_present(db, src_sys_id, region):
        # Check if source system associated with asset
        associated = is_associated_with_asset(db, src_sys_id, region)
        # If it is not associated,source system stack will be deleted
        if not associated:
            del_source_sys_item(db, src_sys_id)
            # delete_src_sys_stack(src_sys_id, region)
        # If it is associated,source system stack will not be deleted as the data asset need to be deleted first
        elif associated:
            logger.write(
                message=f"Source system {src_sys_id} cannot be deleted.Data asset associated with it"
            )
    else:
        # If source system is not present
        logger.write(message=f"Source system {src_sys_id} does not exist")


def del_src_system(db, src_sys_id, region=None):
    """
    Delete the source system stack
    :param db:
    :param src_sys_id:Source system ID
    :param region: Region
    :return: None
    """
    if region is None:
        primary_region = global_config["primary_region"]
        secondary_region = global_config["secondary_region"]
        # If region is not specified,deletion will take place in both the regions
        del_src_sys_with_region(db, src_sys_id, primary_region)
        del_src_sys_with_region(db, src_sys_id, secondary_region)
    else:
        # If region is specified,deletion will take place in only the specified region
        del_src_sys_with_region(db, src_sys_id, region)


def main():
    """
    :return:None
    """
    arguments = sys.argv
    db_secret = global_config['db_secret']
    db_region = global_config['db_region']
    db = Connector(
        secret=db_secret, region=db_region, autocommit=True
    )
    try:
        # If no argument is passed as command line argument
        if len(arguments) < 2:
            logger.write(message="Missing Arguments: src_sys_id and region")
        # If only src_sys_id is passed as command line argument
        elif len(arguments) == 2:
            src_sys_id = arguments[1]
            logger.write(message=f"Attempting to delete source_system: {src_sys_id}")
            del_src_system(db, src_sys_id)
        # If both src_sys_id and region are passed as command line arguments
        elif len(arguments) == 3:
            src_sys_id = arguments[1]
            region = arguments[2]
            logger.write(
                message=f"Attempting to delete source_system: {src_sys_id} in region: {region}"
            )
            del_src_system(db, src_sys_id, region)
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    main()
