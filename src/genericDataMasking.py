import sys
from awsglue.utils import getResolvedOptions

from utils.data_asset import DataAsset
from utils.comUtils import *
from utils.capeprivacyUtils import *

import time


def get_global_config():
    config_file_path = "globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    return config


args = getResolvedOptions(sys.argv, ["source_path", "source_id", "asset_id","exec_id"])
global_config = get_global_config()
start_time = time.time()
asset = DataAsset(args, global_config, run_identifier='data-masking')
spark = get_spark_for_masking(asset.logger)
source_df = create_spark_df(
    spark,
    asset.source_file_path,
    asset.asset_file_type,
    asset.asset_file_delim,
    asset.asset_file_header,
    asset.logger
)
try:
    assert source_df is not None
except AssertionError:
    asset.logger.write("Unable to read the data from the source")
    sys.exit()
asset.update_data_catalog(data_masking='In-Progress')
metadata = asset.get_asset_metadata()
key = get_secret(asset.secret_name, asset.region, asset.logger)
result = run_data_masking(source_df, metadata, key, asset.logger)
try:
    assert result is not None
    target_path = asset.get_masking_path()
    store_sparkdf_to_s3(result,target_path,
                        asset.asset_file_type,
                        asset.asset_file_delim,
                        asset.asset_file_header,
                        asset.logger)
except AssertionError:
    asset.update_data_catalog(data_masking='Failure')
    print("Encountered error while running data masking")
    move_file(asset.source_path, asset.logger)

asset.update_data_catalog(data_masking='Completed')
end_time = time.time()
asset.logger.write(message=f"Time Taken = {round(end_time - start_time, 2)} seconds")
asset.logger.write_logs_to_s3()
stop_spark(spark)
