import sys
import time
from awsglue.utils import getResolvedOptions
from utils.data_asset import DataAsset
from utils.comUtils import *
from utils.capeprivacyUtils import *


def get_global_config():
    config_file_path = "globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    return config


args = getResolvedOptions(sys.argv, ["source_path", "source_id", "asset_id", "exec_id"])
global_config = get_global_config()
start_time = time.time()
asset = DataAsset(args, global_config, run_identifier="data-masking")
spark = get_spark_for_masking(asset.logger)
try:
    source_df = create_spark_df(
        spark,
        asset.source_file_path,
        asset.asset_file_type,
        asset.asset_file_delim,
        asset.asset_file_header,
        asset.logger,
    )
except Exception as e:
    asset.logger.write(message=str(e))
    asset.update_data_catalog(data_masking="Failed")
    asset.logger.write_logs_to_s3()

asset.update_data_catalog(data_masking="In-Progress")
metadata = asset.get_asset_metadata()
key = get_secret(asset.secret_name, asset.region, asset.logger)
result = run_data_masking(source_df, metadata, key, asset.logger)
try:
    result = run_data_masking(source_df, metadata, key, asset.logger)
    target_path = asset.get_masking_path()
    target_path = target_path + get_timestamp(asset.source_file_path) + "/"
    store_sparkdf_to_s3(
        result,
        target_path,
        asset.asset_file_type,
        asset.asset_file_delim,
        asset.asset_file_header,
        asset.logger,
    )
except Exception as e:
    asset.update_data_catalog(data_masking="Failed")
    asset.logger.write(message=str(e))
    move_file(asset.source_path, asset.logger)

asset.update_data_catalog(data_masking="Completed")
end_time = time.time()
asset.logger.write(message=f"Time Taken = {round(end_time - start_time, 2)} seconds")
asset.logger.write_logs_to_s3()
stop_spark(spark)

