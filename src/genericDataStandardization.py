import sys
import time
from awsglue.utils import getResolvedOptions
from utils.data_asset import DataAsset
from utils.comUtils import *
from utils.standardizationUtils import *
from utils.athena_ddl import get_or_create_db, \
    get_or_create_table, manage_partition


def get_global_config():
    config_file_path = "globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    return config


spark = SparkSession.builder.appName('Data-Standardization').getOrCreate()
args = getResolvedOptions(sys.argv, ["source_path", "source_id", "asset_id", "exec_id"])
global_config = get_global_config()
start_time = time.time()
asset = DataAsset(args, global_config, run_identifier="data-standardization")
try:
    source_df = create_spark_df(
        spark,
        asset.source_file_path,
        asset.asset_file_type,
        asset.asset_file_delim,
        asset.asset_file_header,
        asset.logger,
    )
    asset.update_data_catalog(data_standardization="In-Progress")
    metadata = asset.get_asset_metadata()
    result = run_data_standardization(source_df, metadata, asset.logger)
    target_system_info = get_target_system_info(
        asset.fm_prefix, asset.target_id, asset.region, asset.logger
    )
    timestamp = get_timestamp(asset.source_path)
    target_path = get_standardization_path(
        target_system_info, asset.asset_id, timestamp, asset.logger
    )
    result.repartition(1).write.parquet(target_path, mode="overwrite")

    # Storing the target file to Athena with DB = Domain and Table = SUb-domain_AssetId
    domain = target_system_info['Domain']
    get_or_create_db(asset.region, domain)
    athena_path = get_athena_path(target_system_info, asset.asset_id)
    asset.update_data_catalog(data_standardization="Completed")
    get_or_create_table(
        asset.region, result, target_system_info, asset.asset_name, athena_path, True
    )
    manage_partition(
        asset.region, target_system_info, asset.asset_name, timestamp, target_path
    )

except Exception as e:
    asset.logger.write(message=str(e))
    asset.update_data_catalog(data_standardization="Failed")
    asset.logger.write_logs_to_s3()
end_time = time.time()
total_time_taken = float("{0:.2f}".format(end_time - start_time))
asset.logger.write(message=f"Time Taken = {total_time_taken} seconds")
asset.logger.write_logs_to_s3()
stop_spark(spark)
