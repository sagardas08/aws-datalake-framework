"""
== Create a new source system asset
1. Build the directory structure for the asset in associated S3 bucket
2. Setup creatObject event in the associated S3 bucket
3. Insert entry in the dl-fmwrk.data_asset dynamoDb table for the asset
4. Create a new dynamoDb table for the asset and create entries for all columms
"""

import sys
from random import random, randrange
from utils.comUtils import insert_asset_item_rds
from utils.comUtils import getGlobalParams
from utils.comUtils import insert_asset_cols_rds
from utils.comUtils import set_bucket_event_notification
from utils.comUtils import create_src_s3_dir_str


def generate_asset_id(n):
    return int(f'{randrange(1, 10**n):03}')


def main():
    """
    Inserting info in the primary region only
    """
    asset_json_file = sys.argv[1]
    asset_col_json_file = sys.argv[2]
    global_config = getGlobalParams()
    asset_id = generate_asset_id(10)
    print("Insert asset item")
    insert_asset_item_rds(
        asset_json_file, asset_id, global_config["primary_region"]
    )
    print("Insert asset columns")
    insert_asset_cols_rds(
        asset_col_json_file, asset_id, global_config["primary_region"]
    )
    """
    insert_asset_item_rds(
        asset_json_file, asset_id, global_config["secondary_region"]
    )
    insert_asset_cols_rds(
        asset_col_json_file, asset_id, global_config["secondary_region"]
    )
    """
    print("Create s3 directory structure")
    create_src_s3_dir_str(
        asset_id, asset_json_file, global_config["primary_region"]
    )
    create_src_s3_dir_str(
        asset_id, asset_json_file, global_config["secondary_region"]
    )

    print("Add Bucket Notification")
    set_bucket_event_notification(
        asset_id, asset_json_file, global_config["primary_region"]
    )
    set_bucket_event_notification(
        asset_id, asset_json_file, global_config["secondary_region"]
    )


if __name__ == "__main__":
    main()
