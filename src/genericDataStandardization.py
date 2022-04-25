import sys
import time
from awsglue.utils import getResolvedOptions
from utils.data_asset import DataAsset
from utils.comUtils import *
from utils.standardizationUtils import *

def get_global_config():
    """
    Utility method to get global config file
    :return:JSON
    """
    config_file_path = "globalConfig.json"
    file = open(file=config_file_path, mode="r")
    config = json.load(file)
    file.close()
    return config


args = getResolvedOptions(sys.argv, ["source_path", "source_id", "asset_id", "exec_id"])
global_config = get_global_config()
start_time = time.time()
# Creating a spark session object
spark = sql.SparkSession.builder.getOrCreate()
asset = DataAsset(args, global_config, run_identifier="data-standardization")
try:
    # Creating spark dataframe from input file.Supported:CSV,Parquet,JSON,ORC
    source_df = create_spark_df(
        spark,
        asset.source_file_path,
        asset.asset_file_type,
        asset.asset_file_delim,
        asset.asset_file_header,
        asset.logger,
    )
    # Update the data catalog dynamoDB table to "In-Progress" for easy monitoring
    asset.update_data_catalog(data_standardization="In-Progress")
    # Getting data asset table dedicated for a specific asset which specifies if masking is required or not
    metadata = asset.get_asset_metadata()
    # Function to standardize the data to a user required format
    result = run_data_standardization(source_df, metadata, asset.logger)
    # Getting data from target system table
    target_system_info = get_target_system_info(
        asset.fm_prefix, asset.target_id, asset.region, asset.logger
    )
    # Getting the timestamp identifier from the source path
    timestamp = get_timestamp(asset.source_path)
    # Getting the standardization path with the help of info from target system
    target_path = get_standardization_path(
        target_system_info, asset.asset_id, timestamp, asset.logger
    )
    # Writing the standardized data to the target path in parquet format
    result.repartition(1).write.parquet(target_path, mode="overwrite")
    # Storing the target file to Athena with DB = Domain and Table = Sub-domain_AssetId
    domain = target_system_info['domain']
    get_or_create_db(asset.region, domain, asset.logger)
    athena_path = get_athena_path(target_system_info, asset.asset_id)
    # Updating the data catalog table to "Completed" if the standardization is successful
    asset.update_data_catalog(data_standardization="Completed")
    #Create table in Athena
    get_or_create_table(
        asset.region, result, target_system_info, asset.asset_name,
        athena_path, partition=True, encrypt=asset.encryption, logger=asset.logger
    )
    #Add partitions for the table
    manage_partition(
        asset.region, target_system_info, asset.asset_name,
        timestamp, target_path, asset.logger
    )
except Exception as e:
    asset.logger.write(message=str(e))
    # Updating the data catalog table to "Failed" in case of exceptions
    asset.update_data_catalog(data_standardization="Failed")
    asset.logger.write_logs_to_s3()
    
end_time = time.time()
total_time_taken = float("{0:.2f}".format(end_time - start_time))
asset.logger.write(message=f"Time Taken = {total_time_taken} seconds")
asset.logger.write_logs_to_s3()
# Stop the spark session
stop_spark(spark)
