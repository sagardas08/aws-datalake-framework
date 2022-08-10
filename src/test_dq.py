import sys

from utils.comUtils import *
from utils.dqUtils import *
from utils.data_asset import DataAsset
from connector import Connector


def get_global_config():
    """
    Get the globally defined config from the config file
    :return: JSON object
    """
    config_file_path = "globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    return config


def stop(db):
    """
    Method to stop the sparkSession and write logs to S3
    :return: None
    """
    stop_spark(spark)
    end_time = time.time()
    asset.logger.write(
        message=f"Time Taken = {round(end_time - start_time, 2)} seconds"
    )
    asset.logger.write_logs_to_s3()
    db.close()


# Record the start time of the job
start_time = time.time()
# Get the arguments
# args = getResolvedOptions(sys.argv, ["source_path", "source_id", "asset_id", "exec_id"])

args = {
        "asset_id": 7222129030,
        "exec_id": "236880_7222129030_20220808064349",
        "source_id": 236880,
        "source_path": ""

}

global_config = get_global_config()
db_secret = global_config['db_secret']
db_region = global_config['db_region']
conn = Connector(db_secret, db_region, autocommit=True)
# Creating an object to house imp info about the asset in one place
asset = DataAsset(args, global_config, run_identifier="data-quality", conn=conn)
dq_code = asset.generate_dq_code(conn)
print(dq_code)
